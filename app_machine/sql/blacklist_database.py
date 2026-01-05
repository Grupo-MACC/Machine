# -*- coding: utf-8 -*-
"""
Acceso a BD compartida de blacklist (simula futura RDS/Aurora).

Configuración:
    - Se recomienda PostgreSQL (Aurora PostgreSQL en AWS).
    - Puedes sobreescribir la URL con la variable de entorno BLACKLIST_DATABASE_URL.

Nota:
    - Por defecto apunto a un contenedor llamado 'blacklist-db' (Docker DNS).
"""

from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from sql.blacklist_models import BlacklistBase

logger = logging.getLogger(__name__)

BLACKLIST_DATABASE_URL: str = os.getenv(
    "BLACKLIST_DATABASE_URL",
    "postgresql+asyncpg://blacklist:blacklist@blacklist-db:5432/blacklist",
)

engine = create_async_engine(
    BLACKLIST_DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

BlackListSessionMaker = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


async def init_blacklist_db() -> None:
    """
    Crea tablas de la blacklist en la BD compartida.

    Es idempotente: si existen, no falla.
    """
    logger.info("[BLACKLIST-DB] 🗄️ Creando tablas (si no existen)…")
    async with engine.begin() as conn:
        await conn.run_sync(BlacklistBase.metadata.create_all)


@asynccontextmanager
async def blacklist_session() -> AsyncSession:
    """
    Context manager para obtener sesión contra la BD compartida.

    Uso:
        async with blacklist_session() as session:
            ...
    """
    async with BlackListSessionMaker() as session:
        yield session
