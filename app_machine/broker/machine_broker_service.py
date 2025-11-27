import asyncio
import json
import logging
import httpx
from microservice_chassis_grupo2.core.rabbitmq_core import get_channel, declare_exchange, PUBLIC_KEY_PATH, declare_exchange_logs
from aio_pika import Message
from services import machine_service
from consul_client import get_service_url

logger = logging.getLogger(__name__)

async def publish_pieces_done(order_id: int, piece_ids: list[int]):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    payload = {"order_id": order_id, "piece_ids": piece_ids}

    msg = Message(body=json.dumps(payload).encode(), content_type="application/json", delivery_mode=2)
    await exchange.publish(msg, routing_key="piece.done")
    logger.info(f"[MACHINE] 📤 machine.pieces_done → order={order_id} pieces={piece_ids}")
    await publish_to_logger(
        message={
            "message": "Publicado machine.pieces_done",
            "order_id": order_id,
            "piece_ids": piece_ids,
        },
        topic="machine.debug",
    )
    await connection.close()

# ---------- HANDLER: consume machine.do_pieces ----------
async def handle_do_pieces(message):
    async with message.process():
        data = json.loads(message.body)
        order_id  = data.get("order_id")
        piece_ids = data.get("piece_ids", [])
        await machine_service.add_pieces_to_queue(piece_ids)
        
        logger.info(f"[MACHINE] Recibido do.pieces → order={order_id} pieces={piece_ids}")
        await publish_to_logger(
            message={
                "message": "Recibido do.pieces",
                "order_id": order_id,
                "piece_ids": piece_ids,
            },
            topic="machine.info",
        )

# ---------- CONSUMER BOOT ----------
async def consume_do_pieces_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    queue = await channel.declare_queue("do_pieces_queue", durable=True)
    await queue.bind(exchange, routing_key="do.pieces")

    await queue.consume(handle_do_pieces)
    logger.info("[MACHINE] 🟢 Escuchando 'machine.do_pieces' …")
    await publish_to_logger(
        message={"message": "Escuchando machine.do_pieces"},
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