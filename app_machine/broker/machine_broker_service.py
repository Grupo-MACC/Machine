# app_machine/broker/machine_broker_service.py
import json
import logging
from aio_pika import connect_robust, Message, ExchangeType

from .setup_rabbitmq import RABBITMQ_HOST, EXCHANGE_NAME
logger = logging.getLogger(__name__)

# ---------- Publicaciones (Machine produce eventos) ----------

async def publish_machine_job_accepted(order_id: int, piece_ids: list[int], correlation_id: str | None = None):
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)

    headers = {}
    if correlation_id:
        headers["correlationId"] = correlation_id

    msg = Message(
        body=json.dumps({"order_id": order_id, "piece_ids": piece_ids}).encode(),
        content_type="application/json",
        headers=headers,
        delivery_mode=2,  # persistente
    )
    await exchange.publish(msg, routing_key="machine.job.accepted")
    logger.info(f"[MACHINE] 📣 published machine.job.accepted order={order_id} pieces={piece_ids}")
    await connection.close()

async def publish_machine_job_started(order_id: int, piece_ids: list[int], correlation_id: str | None = None):
    from aio_pika import connect_robust, Message, ExchangeType
    import json
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    headers = {"correlationId": correlation_id} if correlation_id else {}
    msg = Message(body=json.dumps({"order_id": order_id, "piece_ids": piece_ids}).encode(),
                  content_type="application/json", headers=headers, delivery_mode=2)
    await exchange.publish(msg, routing_key="machine.job.started")
    logger.info(f"[MACHINE] 📣 published machine.job.started order={order_id} pieces={piece_ids}")
    await connection.close()

async def publish_machine_job_completed(order_id: int, piece_ids: list[int], correlation_id: str | None = None):
    from aio_pika import connect_robust, Message, ExchangeType
    import json, datetime as dt
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    headers = {"correlationId": correlation_id} if correlation_id else {}
    payload = {
        "order_id": order_id,
        "piece_ids": piece_ids,
        "completed_at": dt.datetime.utcnow().isoformat() + "Z"
    }
    msg = Message(body=json.dumps(payload).encode(),
                  content_type="application/json", headers=headers, delivery_mode=2)
    await exchange.publish(msg, routing_key="machine.job.completed")
    logger.info(f"[MACHINE] 📣 published machine.job.completed order={order_id} pieces={piece_ids}")
    await connection.close()

async def publish_machine_job_failed(order_id: int, reason: str, correlation_id: str | None = None):
    from aio_pika import connect_robust, Message, ExchangeType
    import json
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    headers = {"correlationId": correlation_id} if correlation_id else {}
    payload = {"order_id": order_id, "reason": reason}
    msg = Message(body=json.dumps(payload).encode(),
                  content_type="application/json", headers=headers, delivery_mode=2)
    await exchange.publish(msg, routing_key="machine.job.failed")
    logger.info(f"[MACHINE] 📣 published machine.job.failed order={order_id} reason={reason}")
    await connection.close()


# ---------- Consumo (Machine escucha eventos) ----------

async def handle_order_taken(message):
    # Procesa y ACK automático con el context manager
    async with message.process():
        data = json.loads(message.body)
        headers = message.headers or {}
        logger.info(f"[MACHINE] 📥 order.taken recibido: {data} headers={headers}")

        order_id = data.get("orderId")
        piece_ids = data.get("pieceIds", [])
        corr = headers.get("correlationId")

        # 1) Encola piezas en la "máquina" usando tu lógica actual
        #    Import tardío para evitar ciclos
        from business_logic.async_machine import Machine
        machine = await Machine.create()  # o usa tu dependencia get_machine()
        await machine.add_pieces_to_queue(piece_ids)

        # 2) Publica "aceptado"
        await publish_machine_job_accepted(order_id, piece_ids, corr)

async def consume_order_taken_events():
    """
    Declara la cola y se queda consumiendo 'order.taken'.
    """
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    # Asegura exchange/cola/binding
    exchange = await channel.declare_exchange(EXCHANGE_NAME, ExchangeType.TOPIC, durable=True)
    queue = await channel.declare_queue("machine_order_taken_queue", durable=True)
    await queue.bind(exchange, routing_key="order.taken")

    await queue.consume(handle_order_taken)
    logger.info("[MACHINE] 🟢 Escuchando 'order.taken'...")

    # Mantiene la corrutina viva
    import asyncio
    await asyncio.Future()