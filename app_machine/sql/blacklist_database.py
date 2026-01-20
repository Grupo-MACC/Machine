# -*- coding: utf-8 -*-
"""
Acceso a BD de blacklist (ahora integrada en la RDS del chassis).

Nota:
    - La blacklist ya NO usa una BD separada.
    - Usa la misma conexión que el resto del microservicio (vía chassis).
    - Esto permite sincronización entre múltiples máquinas.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from microservice_chassis_grupo2.core.dependencies import get_db

logger = logging.getLogger(__name__)


@asynccontextmanager
async def blacklist_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager para obtener sesión contra la BD (RDS compartida).

    Nota:
        - Ahora usa get_db() del chassis (misma conexión que el resto del servicio).
        - Esto garantiza que la blacklist esté en la misma RDS.

    Uso:
        async with blacklist_session() as session:
            ...
    """
    agen = get_db()
    try:
        session = await anext(agen)
        yield session
    finally:
        await agen.aclose()
