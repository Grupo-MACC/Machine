# -*- coding: utf-8 -*-
"""Main file to start FastAPI application."""
import logging.config
import os
from contextlib import asynccontextmanager
import uvicorn
import asyncio
from fastapi import FastAPI
from broker import machine_broker_service
from routers import machine_router
from sql.blacklist_database import init_blacklist_db
from consul_client import create_consul_client
from microservice_chassis_grupo2.sql import database, models

# Configure logging ################################################################################
# logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"),disable_existing_loggers=False,)
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
        

        # Register with Consul
        result = await consul_client.register_service(
            service_name=service_name,
            service_id=service_id,
            service_port=service_port,
            service_address=service_name,  # Docker DNS
            tags=["fastapi", service_name],
            meta={"version": "2.0.0"},
            health_check_url=f"http://{service_name}:{service_port}/docs"
        )
        logger.info(f"✅ Consul service registration: {result}")

        # Creación de tablas
        try:
            logger.info("[MACHINE] 🗄️ Creando tablas de base de datos")
            async with database.engine.begin() as conn:
                await conn.run_sync(models.Base.metadata.create_all)
                await init_blacklist_db()
        except Exception as exc:
            logger.exception("[MACHINE] ❌ Error creando tablas: %s", exc)

        try:
            task_machine = asyncio.create_task(machine_broker_service.consume_do_pieces_events())
            task_cancel = asyncio.create_task(machine_broker_service.consume_cmd_machine_cancel())
            task_auth = asyncio.create_task(machine_broker_service.consume_auth_events())
        except Exception as e:
            logger.error(f"Error lanzando payment broker service: {e}")
        
        await machine_broker_service.ensure_auth_public_key()

        yield
    finally:
        task_machine.cancel()
        task_cancel.cancel()
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