# app_machine/broker/setup_rabbitmq.py
from aio_pika import connect_robust, ExchangeType

RABBITMQ_HOST = "amqp://guest:guest@localhost/"
EXCHANGE_NAME = "plant.events"  # exchange nuevo para el dominio Machine/Order

async def setup_rabbitmq():
    """
    Crea (si no existen) el exchange 'plant.events' y la cola/binding
    para que Machine escuche 'order.taken'.
    """
    # Conexión robusta con RabbitMQ
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    # Crear el exchange tipo topic
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME, ExchangeType.TOPIC, durable=True
    )

    # Cola de Machine para los pedidos aceptados por Order
    order_taken_queue = await channel.declare_queue(
        "machine_order_taken_queue", durable=True
    )
    await order_taken_queue.bind(exchange, routing_key="order.taken")

    print("✅ RabbitMQ Machine OK (exchange y cola/binding creados).")
    await connection.close()
