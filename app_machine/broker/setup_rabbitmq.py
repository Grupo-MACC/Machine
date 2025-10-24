from aio_pika import connect_robust, ExchangeType

RABBITMQ_HOST = "amqp://guest:guest@rabbitmq/"
ORDER_PAYMENT_EXCHANGE_NAME = "order_payment_exchange"
AUTH_RUNNING_EXCHANGE_NAME = "auth_active_exchange"
EXCHANGE_NAME = "order_payment_exchange"


async def setup_rabbitmq():
    """
    Configura RabbitMQ creando el exchange y las colas necesarias
    usando aio_pika (asíncrono).
    """
    # Conexión robusta con RabbitMQ
    connection = await connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    # Crear el exchange tipo 'topic'
    exchange = await channel.declare_exchange(
        ORDER_PAYMENT_EXCHANGE_NAME,
        ExchangeType.TOPIC,
        durable=True
    )

    # Crear colas
    do_pieces_queue = await channel.declare_queue("do_pieces_queue", durable=True)

    await do_pieces_queue.bind(exchange, routing_key="do.pieces")


    print("✅ RabbitMQ configurado correctamente (exchange + colas creadas).")

    await connection.close()