import asyncio
import json
import logging
import httpx
from microservice_chassis_grupo2.core.rabbitmq_core import get_channel, declare_exchange, PUBLIC_KEY_PATH
from aio_pika import Message
from services import machine_service
from microservice_chassis_grupo2.core.router_utils import AUTH_SERVICE_URL

logger = logging.getLogger(__name__)

async def publish_pieces_done(order_id: int, piece_ids: list[int]):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    payload = {"order_id": order_id, "piece_ids": piece_ids}

    msg = Message(body=json.dumps(payload).encode(), content_type="application/json", delivery_mode=2)
    await exchange.publish(msg, routing_key="piece.done")
    logger.info(f"[MACHINE] 📤 machine.pieces_done → order={order_id} pieces={piece_ids}")

    await connection.close()

# ---------- HANDLER: consume machine.do_pieces ----------
async def handle_do_pieces(message):
    async with message.process():
        data = json.loads(message.body)
        order_id  = data.get("order_id")
        piece_ids = data.get("piece_ids", [])
        await machine_service.add_pieces_to_queue(piece_ids)

# ---------- CONSUMER BOOT ----------
async def consume_do_pieces_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    queue = await channel.declare_queue("do_pieces_queue", durable=True)
    await queue.bind(exchange, routing_key="do.pieces")

    await queue.consume(handle_do_pieces)
    logger.info("[MACHINE] 🟢 Escuchando 'machine.do_pieces' …")
    await asyncio.Future()
    
async def consume_auth_events():
    _, channel = await get_channel()
    
    exchange = await declare_exchange(channel)
    
    machine_queue = await channel.declare_queue('machine_queue', durable=True)
    await machine_queue.bind(exchange, routing_key="auth.running")
    await machine_queue.bind(exchange, routing_key="auth.not_running")
    
    await machine_queue.consume(handle_auth_events)

async def handle_auth_events(message):
    async with message.process():
        data = json.loads(message.body)
        if data["status"] == "running":
            try:
                async with httpx.AsyncClient(
                    verify="/certs/ca.pem",
                    cert=("/certs/machine/machine-cert.pem", "/certs/machine/machine-key.pem"),
                ) as client:
                    response = await client.get(
                        f"{AUTH_SERVICE_URL}/auth/public-key"
                    )
                    response.raise_for_status()
                    public_key = response.text
                    
                    with open(PUBLIC_KEY_PATH, "w", encoding="utf-8") as f:
                        f.write(public_key)
                    
                    logger.info(f"✅ Clave pública de Auth guardada en {PUBLIC_KEY_PATH}")
            except Exception as exc:
                print(exc)

async def publish_message(topic: str, message: dict):
    connection, channel = await get_channel()
    
    exchange = await declare_exchange(channel)

    msg = Message(body=json.dumps(message).encode(), content_type="application/json", delivery_mode=2)
    await exchange.publish(message=msg, routing_key=topic)

    await connection.close()
