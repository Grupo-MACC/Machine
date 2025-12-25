# app_machine/broker/machine_broker_service.py
"""
RabbitMQ broker para Machine.

- Warehouse publica 1 pieza por mensaje a routing keys:
    - machine.a  (piezas tipo A)
    - machine.b  (piezas tipo B)
- Machine consume SOLO su tipo.
- Consumo justo entre réplicas:
    - QoS prefetch_count=1
    - ACK SOLO cuando termina la fabricación
- Al terminar, publica en `piece.done`:
    - order_id
    - piece_id
    - piece_type
    - manufacturing_date (UTC ahora)
"""

import asyncio
import json
import logging
import os
from aio_pika import Message

from microservice_chassis_grupo2.core.rabbitmq_core import (
    get_channel,
    declare_exchange,
    PUBLIC_KEY_PATH,
    declare_exchange_logs,
)

import httpx
from consul_client import get_service_url
from dependencies import get_machine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuración por entorno: te permite reutilizar el MISMO código
# para machine-A / machine-B y para réplicas sin tocar el source.
# ---------------------------------------------------------------------
MACHINE_PIECE_TYPE = os.getenv("MACHINE_PIECE_TYPE", "A")  # "A" o "B"
MACHINE_ROUTING_KEY = os.getenv("MACHINE_ROUTING_KEY", f"machine.{MACHINE_PIECE_TYPE.lower()}")
MACHINE_QUEUE_NAME = os.getenv("MACHINE_QUEUE_NAME", f"machine_{MACHINE_PIECE_TYPE.lower()}_queue")


# ---------- HANDLER: consume piezas UNA A UNA ----------
async def handle_do_pieces(message):
    """
    Consume mensajes con UNA pieza por mensaje.

    Contrato esperado desde Warehouse:
      - piece_id (str UUID)
      - order_id (int)
      - piece_type ('A'|'B')
      - order_date (ISO str)  -> de momento no se usa, pero se acepta.

    IMPORTANTE:
    - Aquí NO se hace ACK hasta que termina la fabricación (sale del context manager).
    - Con prefetch_count=1, esta instancia no recibirá otro mensaje mientras fabrica.
    """
    async with message.process():
        data = json.loads(message.body)

        piece_id = data.get("piece_id")
        order_id = data.get("order_id")
        piece_type = data.get("piece_type")
        order_date = data.get("order_date")  # futuro: logging / DB

        # Validación mínima
        if not piece_id or order_id is None or piece_type not in ("A", "B"):
            logger.error("[MACHINE] ❌ Mensaje inválido: %s", data)
            return

        # Seguridad: este servicio SOLO fabrica su tipo
        if piece_type != MACHINE_PIECE_TYPE:
            logger.error(
                "[MACHINE] ❌ Pieza de tipo %s recibida pero esta machine es tipo %s. Mensaje=%s",
                piece_type, MACHINE_PIECE_TYPE, data
            )
            # Lo ack-eo para no crear bucle infinito.
            # Si quisieras "requeue", estarías reintentando un bug de routing.
            return

        machine = await get_machine()

        # Fabricar (bloqueante async: sleep)
        done_event = await machine.manufacture_piece(
            order_id=order_id,
            piece_id=piece_id,
            order_date=order_date,
            piece_type=piece_type,
        )

        # Publicar pieza terminada
        await publish_message(topic="piece.done", message=done_event)

        logger.info(
            "[MACHINE-%s] ✅ Fabricada piece=%s (order=%s) → piece.done",
            MACHINE_PIECE_TYPE,
            piece_id,
            order_id,
        )
        await publish_to_logger(
            message={"message": "Pieza fabricada", **done_event},
            topic="machine.info",
        )


# ---------- CONSUMER BOOT ----------
async def consume_do_pieces_events():
    """
    Arranca el consumer del tipo de machine configurado.

    Claves para que haya reparto justo entre réplicas:
    - prefetch_count=1
    - ACK al final (cuando termina fabricación)
    """
    _, channel = await get_channel()
    await channel.set_qos(prefetch_count=1)  # <- CRÍTICO

    exchange = await declare_exchange(channel)

    # CRÍTICO: cola diferente por tipo (A/B), pero compartida entre réplicas del mismo tipo
    queue = await channel.declare_queue(MACHINE_QUEUE_NAME, durable=True)
    await queue.bind(exchange, routing_key=MACHINE_ROUTING_KEY)

    await queue.consume(handle_do_pieces)

    logger.info("[MACHINE-%s] 🟢 Escuchando '%s' en cola '%s' …", MACHINE_PIECE_TYPE, MACHINE_ROUTING_KEY, MACHINE_QUEUE_NAME)
    await publish_to_logger(
        message={"message": "Escuchando piezas", "routing_key": MACHINE_ROUTING_KEY, "queue": MACHINE_QUEUE_NAME},
        topic="machine.info",
    )

    await asyncio.Future()
    
async def consume_auth_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    
    machine_queue = await channel.declare_queue('machine_queue', durable=True)
    await machine_queue.bind(exchange, routing_key="auth.running")
    await machine_queue.bind(exchange, routing_key="auth.not_running")
    
    await machine_queue.consume(handle_auth_events)
    logger.info("[MACHINE] 🟢 Escuchando eventos de auth (running/not_running)...")
    await publish_to_logger(
        message={"message": "Escuchando eventos de auth"},
        topic="machine.info",
    )

async def handle_auth_events(message):
    async with message.process():
        data = json.loads(message.body)
        if data["status"] == "running":
            try:
                # Use Consul to discover auth service (no fallback)
                auth_service_url = await get_service_url("auth")
                logger.info(f"[MACHINE] 🔍 Auth descubierto via Consul: {auth_service_url}")
                
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"{auth_service_url}/auth/public-key"
                    )
                    response.raise_for_status()
                    public_key = response.text
                    
                    with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                        f.write(public_key)
                    
                    logger.info(f"✅ Clave pública de Auth guardada en {PUBLIC_KEY_PATH}")
                    await publish_to_logger(
                        message={
                            "message": "Clave pública de Auth guardada",
                            "path": PUBLIC_KEY_PATH,
                        },
                        topic="machine.info",
                    )
            except Exception as exc:
                logger.error(f"[MACHINE] ❌ Error obteniendo clave pública de Auth: {exc}")
                await publish_to_logger(
                    message={
                        "message": "Error obteniendo clave pública de Auth",
                        "error": str(exc),
                    },
                    topic="machine.error",
                )

async def publish_message(topic: str, message: dict):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    msg = Message(body=json.dumps(message).encode(), content_type="application/json", delivery_mode=2)
    await exchange.publish(message=msg, routing_key=topic)

    await connection.close()

async def publish_to_logger(message: dict, topic: str):
    """
    Envía un log estructurado al sistema de logs.
    topic: 'machine.info', 'machine.error', 'machine.debug', etc.
    """
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange_logs(channel)

        log_data = {
            "measurement": "logs",
            "service": topic.split(".")[0],   # 'machine'
            "severity": topic.split(".")[1],  # 'info', 'error', 'debug'...
            **message,
        }

        msg = Message(
            body=json.dumps(log_data).encode(),
            content_type="application/json",
            delivery_mode=2,
        )

        await exchange.publish(message=msg, routing_key=topic)
    except Exception as e:
        print(f"[MACHINE] Error publishing to logger: {e}")
    finally:
        if connection:
            await connection.close()