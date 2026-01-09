# app_machine/sql/models.py
# -*- coding: utf-8 -*-
"""
Modelos SQLAlchemy para Machine.

Tablas:
- manufactured_piece: histórico de piezas finalizadas (y opcionalmente saltadas).
- inflight_piece: estado persistente de la pieza en curso (1 fila).
"""

from datetime import datetime, timezone
from sqlalchemy import String, Integer, DateTime, Boolean, Text
from sqlalchemy.orm import Mapped, mapped_column
from microservice_chassis_grupo2.sql.models import BaseModel


def utcnow() -> datetime:
    """Devuelve datetime UTC aware."""
    return datetime.now(timezone.utc)


class ManufacturedPiece(BaseModel):
    """Histórico de piezas procesadas por esta instancia."""
    __tablename__ = "manufactured_piece"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    piece_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    order_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    piece_type: Mapped[str] = mapped_column(String(1), nullable=False)  # 'A' o 'B'

    order_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    manufacturing_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Útil para reinicios: saber si ya publicaste el evento piece.done
    done_published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Opcional pero recomendable: auditar si se saltó por cancelación
    result: Mapped[str] = mapped_column(String(32), nullable=False, default="MANUFACTURED")  # o 'SKIPPED'
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class InflightPiece(BaseModel):
    """
    Estado persistente de la pieza en curso.

    Nota:
    - Solo hay 1 en curso por instancia (prefetch=1).
    - Usamos una única fila con PK fija = 1.
    """
    __tablename__ = "inflight_piece"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    piece_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    order_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    piece_type: Mapped[str] = mapped_column(String(1), nullable=False)

    order_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    duration_s: Mapped[int] = mapped_column(Integer, nullable=False)

    done_published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
