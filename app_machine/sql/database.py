# app_machine/sql/database.py
# -*- coding: utf-8 -*-
"""
Capa de acceso a base de datos (Async SQLAlchemy + SQLite).

Objetivo:
- Cada instancia de machine tiene su propia DB (por defecto SQLite en fichero).
- Inicializa tablas al arrancar.
- Provee un sessionmaker async para usar desde lógica y endpoints.
"""

import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base

# Base declarativa para modelos
Base = declarative_base()

# DB por instancia (puedes sobreescribir con env var)
# Nota: usa una ruta persistente (monta volumen en docker-compose).
MACHINE_DATABASE_URL = os.getenv(
    "MACHINE_DATABASE_URL",
    "sqlite+aiosqlite:////home/pyuser/code/app_machine/machine.db",
)

engine = create_async_engine(
    MACHINE_DATABASE_URL,
    echo=False,  # pon True si quieres debug SQL
)

SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

async def init_db() -> None:
    """
    Crea tablas si no existen.

    Importante:
    - Debe llamarse en startup/lifespan antes de usar modelos.
    """
    from sql import models  # noqa: F401  (asegura que se registran modelos en Base.metadata)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
