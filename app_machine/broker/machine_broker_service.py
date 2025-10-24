import asyncio
import json
import logging
import httpx
from aio_pika import connect_robust, Message, ExchangeType
from broker.setup_rabbitmq import RABBITMQ_HOST, EXCHANGE_NAME
from services import machine_service
logger = logging.getLogger(__name__)

async def publish_pieces_done(order_id: int, piece_ids: list[int]):
    connection = await connect_robust(RABBITMQ_HOST)
    channel     = await connection.channel()
    exchange    = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)

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
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue("do_pieces_queue", durable=True)
    await queue.bind(exchange, routing_key="do.pieces")

    await queue.consume(handle_do_pieces)
    logger.info("[MACHINE] 🟢 Escuchando 'machine.do_pieces' …")
    await asyncio.Future()

async def publish_message(topic: str, message: dict):
    connection = await connect_robust(RABBITMQ_HOST)
    channel     = await connection.channel()
    exchange    = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)


    msg = Message(body=json.dumps(message).encode(), content_type="application/json", delivery_mode=2)
    await exchange.publish(message=msg, routing_key=topic)

    await connection.close()
