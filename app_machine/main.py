# -*- coding: utf-8 -*-
"""Main file to start FastAPI application."""
import logging.config
import os
from contextlib import asynccontextmanager
import uvicorn
import asyncio
from fastapi import FastAPI
from broker import setup_rabbitmq, machine_broker_service
from routers import machine_router
from microservice_chassis_grupo2.core.consul import create_consul_client
from microservice_chassis_grupo2.sql import database, models


# Configure logging ################################################################################
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logger = logging.getLogger(__name__)


# App Lifespan #####################################################################################
@asynccontextmanager
async def lifespan(__app: FastAPI):
    """Lifespan context manager."""
    consul_client = create_consul_client()
    service_id = os.getenv("SERVICE_ID", "machine-1")
    service_name = os.getenv("SERVICE_NAME", "machine")
    service_port = int(os.getenv("SERVICE_PORT", 5001))

    try:
        logger.info("Starting up")
        # ✅ PRIMERO: Inicializar la base de datos
        logger.info("Initializing database connection")
        await database.init_database()
        logger.info("Database connection initialized")
        '''try:
            await setup_rabbitmq.setup_rabbitmq()
        except Exception as e:
            logger.error(f"Error configurando RabbitMQ: {e}")'''
        try:
            task_machine = asyncio.create_task(machine_broker_service.consume_do_pieces_events())
            task_auth = asyncio.create_task(machine_broker_service.consume_auth_events())
        except Exception as e:
            logger.error(f"Could not create tables at startup")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error message: {str(e)}")
            logger.error(f"Traceback:", exc_info=True)
        yield
    finally:
        task_machine.cancel()
        task_auth.cancel()
        
        # Deregister from Consul
        result = await consul_client.deregister_service(service_id)
        logger.info(f"✅ Consul service deregistration: {result}")

# OpenAPI Documentation ############################################################################
APP_VERSION = os.getenv("APP_VERSION", "2.0.0")
logger.info("Running app version %s", APP_VERSION)
app = FastAPI(
    redoc_url=None,  # disable redoc documentation.
    version=APP_VERSION,
    servers=[{"url": "/", "description": "Development"}],
    license_info={
        "name": "MIT License",
        "url": "https://choosealicense.com/licenses/mit/",
    },
    lifespan=lifespan,
)

app.include_router(machine_router.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5001, reload=True)

#python -m uvicorn main:app --reload --port 5000