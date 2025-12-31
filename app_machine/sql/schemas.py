# -*- coding: utf-8 -*-
"""
Schemas Pydantic para exponer datos de Machine.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, ConfigDict, Field

class Message(BaseModel):
    """Message schema definition."""
    detail: Optional[str] = Field(example="error or success message")

class OrderBase(BaseModel):
    """Order base schema definition."""
    number_of_pieces: int = Field(
        description="Number of pieces to manufacture for the new order",
        default=None,
        example=10
    )
    description: str = Field(
        description="Human readable description for the order",
        default="No description",
        example="CompanyX order on 2022-01-20"
    )


class Order(OrderBase):
    """Order schema definition."""
    model_config = ConfigDict(from_attributes=True)  # ORM mode ON
    id: int = Field(
        description="Primary key/identifier of the order.",
        default=None,
        example=1
    )
    status: str = Field(
        description="Current status of the order",
        default="Created",
        example="Finished"
    )


class OrderPost(OrderBase):
    """Schema definition to create a new order."""


class PieceBase(BaseModel):
    """Piece base schema definition."""
    id: int = Field(
        description="Piece identifier (Primary key)",
        example="1"
    )
    manufacturing_date: Optional[datetime] = Field(
        description="Date when piece has been manufactured",
        example="2022-07-22T17:32:32.193211"
    )
    status: str = Field(
        description="Current status of the piece",
        default="Queued",
        example="Manufactured"
    )


class Piece(PieceBase):
    """Piece schema definition."""
    model_config = ConfigDict(from_attributes=True)  # ORM mode ON
    order: Optional[Order] = Field(description="Order where the piece belongs to")


class ManufacturedPieceOut(BaseModel):
    """
    Respuesta de una pieza procesada por Machine.

    Nota:
    - 'result' te permite extender a futuro (MANUFACTURED / SKIPPED).
    - 'done_published' te sirve para depurar reinicios y publicación de eventos.
    """
    model_config = ConfigDict(from_attributes=True)

    piece_id: str = Field(..., description="UUID de la pieza")
    order_id: int = Field(..., description="ID de la orden")
    piece_type: str = Field(..., description="Tipo de pieza (A/B)")
    order_date: Optional[datetime] = Field(None, description="Fecha de la orden")
    manufacturing_date: Optional[datetime] = Field(None, description="Fecha de fabricación")
    done_published: bool = Field(..., description="Si se publicó piece.done")
    result: str = Field(..., description="MANUFACTURED o SKIPPED")
    reason: Optional[str] = Field(None, description="Motivo si no se fabricó")


class OrderSummaryOut(BaseModel):
    """
    Resumen de producción agrupado por order_id.
    """
    order_id: int
    manufactured_count: int


class InflightPieceOut(BaseModel):
    """
    Estado persistido de la pieza en curso.

    Útil para depurar reanudación tras reinicio.
    """
    model_config = ConfigDict(from_attributes=True)

    piece_id: str
    order_id: int
    piece_type: str
    order_date: Optional[datetime]
    started_at: datetime
    duration_s: int
    done_published: bool
