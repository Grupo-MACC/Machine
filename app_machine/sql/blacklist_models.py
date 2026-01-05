# -*- coding: utf-8 -*-
"""
Modelos SQLAlchemy para la blacklist compartida (entre réplicas).

Idea:
    - Esta base y sus tablas NO deben mezclarse con la metadata de la DB local de Machine.
    - Así evitamos que tablas como inflight_piece (por instancia) terminen en una DB compartida.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    """Devuelve datetime UTC aware."""
    return datetime.now(timezone.utc)


class BlacklistBase(DeclarativeBase):
    """
    Base declarativa independiente.

    Importante:
        - Separada de la Base usada por microservice_chassis_grupo2.
        - Esto permite tener 2 engines y 2 'metadatas' sin crear tablas cruzadas.
    """
    pass


class OrderBlacklistEntry(BlacklistBase):
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
