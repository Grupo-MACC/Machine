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
from consul_client import get_consul_client
from microservice_chassis_grupo2.sql import database, models

# Configure logging ################################################################################
# logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"))
logging.config.fileConfig(os.path.join(os.path.dirname(__file__), "logging.ini"),disable_existing_loggers=False,)
logger = logging.getLogger(__name__)


# App Lifespan #####################################################################################
@asynccontextmanager
async def lifespan(__app: FastAPI):
    """Lifespan context manager."""
    consul = get_consul_client()

    try:
        logger.info("Starting up")
        
        # Registro "auto" (usa SERVICE_* y CONSUL_* desde entorno)
        ok = await consul.register_self()
        logger.info("✅ Consul register_self: %s", ok)

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
        yield
    finally:
        task_machine.cancel()
        task_cancel.cancel()
        task_auth.cancel()
        
        # Deregistro (auto) + cierre del cliente HTTP
        try:
            ok = await consul.deregister_self()
            logger.info("✅ Consul deregister_self: %s", ok)
        except Exception:
            logger.exception("Error desregistrando en Consul")

        try:
            await consul.aclose()
        except Exception:
            logger.exception("Error cerrando cliente Consul")

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
    """
    Application entry point. Starts the Uvicorn server with SSL configuration.
    Runs the FastAPI application on host.
    """
    cert_file = os.getenv("SERVICE_CERT_FILE", "/certs/machine/machine-cert.pem")
    key_file = os.getenv("SERVICE_KEY_FILE", "/certs/machine/machine-key.pem")

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "5001")),
        reload=True,
        ssl_certfile=cert_file,
        ssl_keyfile=key_file,
    )