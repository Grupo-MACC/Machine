# -*- coding: utf-8 -*-
"""RabbitMQ broker para Machine.

Contrato (manteniendo funcionalidad original):
    - Warehouse publica 1 pieza por mensaje:
        * machine.a (piezas tipo A)
        * machine.b (piezas tipo B)
      Machine consume SOLO el tipo configurado por entorno.
    - Reparto justo entre réplicas:
        * QoS prefetch_count=1
        * ACK SOLO cuando termina la fabricación (salida del context manager)
    - Al terminar fabricación publica:
        * piece.done (payload devuelto por Machine.fabricate_piece)
    - SAGA cancelación fabricación:
        * consume cmd.machine.cancel
        * añade order_id a blacklist (DB compartida)
        * publica evt.machine.canceled como confirmación a Warehouse
    - Auth events:
        * consume auth.running / auth.not_running
        * si running, descarga public-key y la guarda en PUBLIC_KEY_PATH
    - Logger:
        * publica logs estructurados a exchange_logs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import httpx
from aio_pika import Message

from consul_client import get_consul_client
from dependencies import get_machine
from microservice_chassis_grupo2.core.rabbitmq_core import (
    PUBLIC_KEY_PATH,
    declare_exchange,
    declare_exchange_logs,
    get_channel,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Constantes RabbitMQ (routing keys / colas / topics)
# =============================================================================

# --- Config por entorno (mismo código para machine-A / machine-B) ---
MACHINE_PIECE_TYPE = os.getenv("MACHINE_PIECE_TYPE", "A")  # "A" o "B"
SERVICE_ID = os.getenv("SERVICE_ID", "machine-1")

# Routing key que esta instancia consume (machine.a o machine.b)
RK_CMD_DO_PIECE = f"todo.machine.{MACHINE_PIECE_TYPE.lower()}"

# Cola compartida entre réplicas del mismo tipo (competición)
QUEUE_DO_PIECE = f"machine_{MACHINE_PIECE_TYPE.lower()}_queue"

# --- Eventos y comandos de la SAGA cancelación (Warehouse -> Machine -> Warehouse) ---
RK_CMD_MACHINE_CANCEL = "cmd.machine.cancel"
RK_EVT_MACHINE_CANCELED = "evt.machine.canceled"

# Cola compartida entre réplicas: solo 1 procesa cada cmd.machine.cancel
QUEUE_MACHINE_CANCEL = "machine_cancel_queue"

# --- Evento de pieza terminada (Machine -> Warehouse) ---
RK_EVT_PIECE_DONE = "piece.done"

# --- Auth events (exchange general) ---
RK_AUTH_RUNNING = "auth.running"
RK_AUTH_NOT_RUNNING = "auth.not_running"
QUEUE_AUTH_EVENTS = "machine_queue"  # se mantiene por compatibilidad (nombre histórico)

# --- Logger topics ---
TOPIC_INFO = "machine.info"
TOPIC_ERROR = "machine.error"
TOPIC_DEBUG = "machine.debug"


# =============================================================================
# 1) HELPERS internos (publicación consistente)
# =============================================================================
#region 0. HELPERS
def _build_json_message(payload: dict) -> Message:
    """Construye un mensaje JSON persistente."""
    return Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        delivery_mode=2,  # persistente
    )


async def _publish_exchange(routing_key: str, payload: dict) -> None:
    """Publica un payload JSON al exchange general (declare_exchange).

    Nota:
        Mantengo el mismo exchange que tu versión original para no cambiar
        semántica/infra de routing.
    """
    connection, channel = await get_channel()
    try:
        exchange = await declare_exchange(channel)
        await exchange.publish(message=_build_json_message(payload), routing_key=routing_key)
    finally:
        await connection.close()


def _require_fields(data: dict, required: tuple[str, ...], context: str) -> bool:
    """Valida que existan campos obligatorios en el payload."""
    missing = [k for k in required if data.get(k) is None]
    if not missing:
        return True
    logger.error("[MACHINE] ❌ Payload inválido en %s (faltan %s): %s", context, missing, data)
    return False


# =============================================================================
# 2) HANDLER: DO PIECES (Warehouse -> Machine)
# =============================================================================
#region 1. FABRICATION
async def handle_do_pieces(message) -> None:
    """Consume 1 pieza por mensaje y publica `piece.done` cuando se fabrica.

    Contrato esperado (Warehouse):
        - piece_id: str UUID
        - order_id: int
        - piece_type: 'A'|'B'
        - order_date: str ISO (opcional; hoy no se usa pero se acepta)

    Propiedades importantes:
        - ACK al final del bloque `async with message.process()`:
          si fabricas lento, no se ACKea hasta terminar.
        - prefetch_count=1: reparto justo entre réplicas.
    """
    async with message.process():
        data = json.loads(message.body)

        if not _require_fields(data, ("piece_id", "order_id", "piece_type"), context="do_piece"):
            return

        piece_id = data.get("piece_id")
        order_id = int(data.get("order_id"))
        piece_type = data.get("piece_type")
        order_date = data.get("order_date")

        if piece_type not in ("A", "B"):
            logger.error("[MACHINE] ❌ piece_type inválido: %s (data=%s)", piece_type, data)
            return

        machine = await get_machine()

        # 1) Idempotencia: si ya está procesada, ACK y fuera (no republish)
        if await machine.is_piece_already_processed(piece_id):
            logger.info("[MACHINE] ♻️ Duplicado piece_id=%s → ACK sin publicar", piece_id)
            return

        # 2) Blacklist: si la order está cancelada, registrar/consumir sin publicar
        if await machine.is_order_blacklisted(order_id):
            logger.info("[MACHINE] 🚫 Order %s cancelada → skip piece %s", order_id, piece_id)
            # Mantengo tu comportamiento: llamas fabricate_piece (posible registro SKIPPED)
            await machine.fabricate_piece(order_id, piece_id, piece_type, order_date)
            return

        # 3) Fabricar (persistirá inflight + fabricated)
        done_event = await machine.fabricate_piece(order_id, piece_id, piece_type, order_date)

        # Si decide no fabricar (blacklist/duplicado), no publicamos
        if not done_event or done_event.get("result") != "MANUFACTURED":
            return

        # 4) Publicar evento de pieza fabricada
        await _publish_exchange(routing_key=RK_EVT_PIECE_DONE, payload=done_event)

        # 5) Marcar como publicado en DB
        await machine.mark_done_published(piece_id)


async def consume_do_pieces_events() -> None:
    """Arranca el consumer del tipo configurado (A/B).

    Claves para reparto justo:
        - prefetch_count=1
        - ACK al final del handler
    """
    connection, channel = await get_channel()
    try:
        await channel.set_qos(prefetch_count=1)  # CRÍTICO

        exchange = await declare_exchange(channel)

        queue = await channel.declare_queue(QUEUE_DO_PIECE, durable=True)
        await queue.bind(exchange, routing_key=RK_CMD_DO_PIECE)
        await queue.consume(handle_do_pieces)

        logger.info(
            "[MACHINE-%s] 🟢 Escuchando '%s' en cola '%s' …",
            MACHINE_PIECE_TYPE,
            RK_CMD_DO_PIECE,
            QUEUE_DO_PIECE,
        )

        await publish_to_logger(
            message={"message": "Escuchando piezas", "routing_key": RK_CMD_DO_PIECE, "queue": QUEUE_DO_PIECE},
            topic=TOPIC_INFO,
        )

        await asyncio.Future()
    finally:
        # Normalmente no se llega (Future infinito)
        await connection.close()


# =============================================================================
# 3) HANDLER: CANCEL (Warehouse -> Machine -> Warehouse)
# =============================================================================
#region 2. CANCEL SAGA
async def handle_cmd_machine_cancel(message) -> None:
    """Procesa cmd.machine.cancel.

    Payload esperado:
        {"order_id": int, "saga_id": str (opcional)}

    Efecto:
        - Inserta order_id en blacklist (DB compartida).
        - Publica evt.machine.canceled como confirmación hacia Warehouse.
    """
    async with message.process():
        data = json.loads(message.body)

        if not _require_fields(data, ("order_id",), context=RK_CMD_MACHINE_CANCEL):
            return

        order_id = int(data.get("order_id"))
        saga_id = data.get("saga_id")

        machine = await get_machine()
        await machine.add_to_blacklist(order_id, reason="CANCEL_MANUFACTURING")

        logger.warning("[MACHINE] 🛑 Cancel registrada en blacklist: order_id=%s", order_id)

        payload = {
            "order_id": order_id, 
            "machine_type": MACHINE_PIECE_TYPE
            }
        
        if saga_id is not None:
            payload["saga_id"] = str(saga_id)

        logger.info("[MACHINE] 📣 Publicando evt.machine.canceled: %s", payload)
        await _publish_exchange(routing_key=RK_EVT_MACHINE_CANCELED, payload=payload)


async def consume_cmd_machine_cancel() -> None:
    """Escucha cmd.machine.cancel en una cola compartida.

    Semántica:
        - Réplicas compiten por la cola -> solo una procesa el comando.
        - Como la blacklist está en BD compartida, el efecto es global.
    """
    connection, channel = await get_channel()
    try:
        exchange = await declare_exchange(channel)

        queue = await channel.declare_queue(QUEUE_MACHINE_CANCEL, durable=True)
        await queue.bind(exchange, routing_key=RK_CMD_MACHINE_CANCEL)
        await queue.consume(handle_cmd_machine_cancel)

        logger.info(
            "[MACHINE] 🟠 Escuchando '%s' en cola '%s' (competing consumers)",
            RK_CMD_MACHINE_CANCEL,
            QUEUE_MACHINE_CANCEL,
        )

        await publish_to_logger(
            message={"message": "Escuchando cmd.machine.cancel", "routing_key": RK_CMD_MACHINE_CANCEL, "queue": QUEUE_MACHINE_CANCEL},
            topic=TOPIC_INFO,
        )

        await asyncio.Future()
    finally:
        await connection.close()


# =============================================================================
# 4) AUTH EVENTS (Auth -> Machine)
# =============================================================================
#region 3. AUTH EVENTS
async def handle_auth_events(message) -> None:
    """Consume auth.running/auth.not_running.

    Solo cuando status == 'running':
        - Descubre Auth via Consul
        - Descarga public key
        - Guarda en PUBLIC_KEY_PATH
    """
    async with message.process():
        data = json.loads(message.body)

        status = data.get("status")
        if status != "running":
            return

        try:
            auth_service_url = await get_consul_client().get_service_base_url("auth")
            logger.info("[MACHINE] 🔍 Auth descubierto via Consul: %s", auth_service_url)

            async with httpx.AsyncClient() as client:
                response = await client.get(f"{auth_service_url}/auth/public-key")
                response.raise_for_status()
                public_key = response.text

            with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                f.write(public_key)

            logger.info("[MACHINE] ✅ Clave pública de Auth guardada en %s", PUBLIC_KEY_PATH)
            await publish_to_logger(
                message={"message": "Clave pública de Auth guardada", "path": PUBLIC_KEY_PATH},
                topic=TOPIC_INFO,
            )
        except Exception as exc:
            logger.error("[MACHINE] ❌ Error obteniendo clave pública de Auth: %s", exc, exc_info=True)
            await publish_to_logger(
                message={"message": "Error obteniendo clave pública de Auth", "error": str(exc)},
                topic=TOPIC_ERROR,
            )


async def consume_auth_events() -> None:
    """Consumer de auth.running/auth.not_running.

    Importante:
        En tu versión original este consumer NO bloqueaba (faltaba Future),
        lo que puede dejarlo inactivo si se ejecuta como task.
    """
    connection, channel = await get_channel()
    try:
        exchange = await declare_exchange(channel)

        queue = await channel.declare_queue(QUEUE_AUTH_EVENTS, durable=True)
        await queue.bind(exchange, routing_key=RK_AUTH_RUNNING)
        await queue.bind(exchange, routing_key=RK_AUTH_NOT_RUNNING)
        await queue.consume(handle_auth_events)

        logger.info("[MACHINE] 🟢 Escuchando eventos de auth (running/not_running) en %s", QUEUE_AUTH_EVENTS)
        await publish_to_logger(
            message={"message": "Escuchando eventos de auth", "queue": QUEUE_AUTH_EVENTS},
            topic=TOPIC_INFO,
        )

        await asyncio.Future()
    finally:
        await connection.close()


# =============================================================================
# 5) LOGGER
# =============================================================================
#region 4. LOGGER
async def publish_to_logger(message: dict, topic: str) -> None:
    """Envía un log estructurado al sistema de logs.

    Args:
        message: dict con campos extra.
        topic: 'machine.info' | 'machine.error' | 'machine.debug' | ...
    """
    connection = None
    try:
        connection, channel = await get_channel()
        exchange = await declare_exchange_logs(channel)

        service, severity = (topic.split(".", 1) + ["info"])[:2]

        log_data = {
            "measurement": "logs",
            "service": service,
            "severity": severity,
            **message,
        }

        await exchange.publish(message=_build_json_message(log_data), routing_key=topic)

    except Exception:
        logger.exception("[MACHINE] Error publicando al logger")
    finally:
        if connection:
            await connection.close()
