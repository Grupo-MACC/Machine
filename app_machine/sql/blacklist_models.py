# -*- coding: utf-8 -*-
"""
Modelos SQLAlchemy para la blacklist compartida (entre réplicas).

Nota:
    - Ahora usa la misma Base que el chassis (microservice_chassis_grupo2).
    - Esto permite que la tabla order_blacklist viva en la RDS junto con las demás tablas.
    - Ideal para escenarios multi-máquina donde se necesita una única fuente de verdad.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

from microservice_chassis_grupo2.sql.models import BaseModel


def utcnow() -> datetime:
    """Devuelve datetime UTC aware."""
    return datetime.now(timezone.utc)


class OrderBlacklistEntry(BaseModel):
    """
    Registro de order_id canceladas (blacklist compartida).

    Reglas:
        - PK = order_id (idempotencia natural)
        - No se elimina en el flujo normal (solo mantenimiento/admin si se decide)
    """
    __tablename__ = "order_blacklist"

    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
