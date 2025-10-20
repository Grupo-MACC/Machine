# -*- coding: utf-8 -*-
"""Main file to start FastAPI application."""
import logging.config
import os
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI
import asyncio
from routers import machine_router
from sql import models
from sql import database
from broker import machine_broker_service, setup_rabbitmq

# Configure logging ################################################################################
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logger = logging.getLogger(__name__)


# App Lifespan #####################################################################################
@asynccontextmanager
async def lifespan(__app: FastAPI):
    """Lifespan context manager."""
    global _task_consumer
    try:
        logger.info("Starting up")

        # 1) DB
        try:
            logger.info("Creating database tables")
            async with database.engine.begin() as conn:
                await conn.run_sync(models.Base.metadata.create_all)
        except Exception:
            logger.error("Could not create tables at startup")

        # 2) RabbitMQ: declarar exchange/colas/bindings de Machine
        try:
            await setup_rabbitmq.setup_rabbitmq()
        except Exception as e:
            logger.error(f"❌ Error configurando RabbitMQ: {e}")

        # 3) Lanzar consumidor (no bloquea FastAPI)
        try:
            _task_consumer = asyncio.create_task(
                machine_broker_service.consume_order_taken_events()
            )
            logger.info("[MACHINE] 🟢 RabbitMQ listo y consumidor lanzado.")
        except Exception as e:
            logger.error(f"❌ Error lanzando machine broker service: {e}")

        yield

    finally:
        logger.info("Shutting down database")
        await database.engine.dispose()

        logger.info("Shutting down RabbitMQ consumer")
        if _task_consumer:
            _task_consumer.cancel()


# OpenAPI Documentation ############################################################################
APP_VERSION = os.getenv("APP_VERSION", "2.0.0")
logger.info("Running app version %s", APP_VERSION)
DESCRIPTION = """
Monolithic manufacturing order application.
"""

tag_metadata = [
    {
        "name": "Machine",
        "description": "Endpoints related to machines",
    },
    {
        "name": "Order",
        "description": "Endpoints to **CREATE**, **READ**, **UPDATE** or **DELETE** orders.",
    },
    {
        "name": "Piece",
        "description": "Endpoints **READ** piece information.",
    },
]

app = FastAPI(
    redoc_url=None,  # disable redoc documentation.
    title="FastAPI - Monolithic app",
    description=DESCRIPTION,
    version=APP_VERSION,
    servers=[{"url": "/", "description": "Development"}],
    license_info={
        "name": "MIT License",
        "url": "https://choosealicense.com/licenses/mit/",
    },
    openapi_tags=tag_metadata,
    lifespan=lifespan,
)

app.include_router(machine_router.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5001, reload=True)

#python -m uvicorn main:app --reload --port 5000