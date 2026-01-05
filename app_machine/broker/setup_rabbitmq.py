# -*- coding: utf-8 -*-
"""
Bootstrap de RabbitMQ para el microservicio Machine.

Responsabilidad:
    - Declarar el exchange (topic) usado por el microservicio.
    - Declarar colas durables necesarias.
    - Crear bindings (routing keys -> colas).

Notas:
    - Este setup es idempotente: declarar exchange/colas existentes no rompe nada.
    - Importante separar:
        - nombre de cola (queue name)
        - routing key (routing key)
"""

import logging
from aio_pika import connect_robust, ExchangeType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Colas + routing keys (config estática)
# ---------------------------------------------------------------------

RABBITMQ_URL: str = "amqp://guest:guest@rabbitmq/"

# Exchange principal del sistema (mantengo el nombre original por compatibilidad)
EXCHANGE_NAME: str = "order_payment_exchange"
EXCHANGE_TYPE = ExchangeType.TOPIC

# Colas y routing keys (declarativo)
DO_PIECES_QUEUE: str = "do_pieces_queue"
RK_DO_PIECES: str = "do.pieces"

# Lista declarativa de bindings: (queue_name, [routing_keys...])
QUEUE_BINDINGS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (DO_PIECES_QUEUE, (RK_DO_PIECES,)),
)


async def setup_rabbitmq() -> None:
    """
    Configura RabbitMQ para Machine.

    Crea:
        - Exchange (topic) durable.
        - Colas durables.
        - Bindings definidos en QUEUE_BINDINGS.
    """
    connection = await connect_robust(RABBITMQ_URL)
    try:
        channel = await connection.channel()

        exchange = await channel.declare_exchange(EXCHANGE_NAME, EXCHANGE_TYPE, durable=True,)

        # Declarar colas y bindings
        for queue_name, routing_keys in QUEUE_BINDINGS:
            queue = await channel.declare_queue(queue_name, durable=True)
            for rk in routing_keys:
                await queue.bind(exchange, routing_key=rk)

        logger.info("✅ RabbitMQ configurado (exchange=%s, bindings=%s).", EXCHANGE_NAME, QUEUE_BINDINGS)

    finally:
        await connection.close()
