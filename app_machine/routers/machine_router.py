# app_machine/routers/machine_router.py
# -*- coding: utf-8 -*-
"""
Endpoints HTTP para consultar estado y BD de Machine.

Cambios aplicados:
- Eliminados imports duplicados.
- Eliminado SessionLocal (ya no existe / no se usa).
- Todos los accesos a BD usan db: AsyncSession = Depends(get_db).
- Aplicado el filtro 'result' que ya estaba declarado en el endpoint /pieces.
"""

import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from microservice_chassis_grupo2.core.dependencies import (
    get_current_user,
    get_db,
    check_public_key,
)

from sql.models import ManufacturedPiece, InflightPiece
from sql import schemas
from business_logic.async_machine import Machine
from dependencies import get_machine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/machine")


@router.get("/health")
async def health_check():
    """
    Healthcheck básico: confirma que el servicio está arriba y con clave pública cargada.
    """
    logger.debug("GET '/health' endpoint called.")
    if check_public_key():
        return {"detail": "OK"}
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Service not available",
    )


@router.get("/status")
async def get_machine_status(
    machine: Machine = Depends(get_machine),
    user: int = Depends(get_current_user),
):
    """
    Devuelve el status en memoria de la máquina (Waiting/Working).
    """
    return {"status": machine.status, "working_piece": machine.working_piece}


@router.get("/pieces", response_model=List[schemas.ManufacturedPieceOut])
async def list_manufactured_pieces(
    order_id: Optional[int] = Query(default=None, description="Filtrar por order_id"),
    result: Optional[str] = Query(default=None, description="Filtrar por result (MANUFACTURED/SKIPPED)"),
    limit: int = Query(default=100, ge=1, le=500, description="Límite de filas"),
    offset: int = Query(default=0, ge=0, description="Offset de paginación"),
    db: AsyncSession = Depends(get_db),
    user: int = Depends(get_current_user),
):
    """
    Lista piezas procesadas por esta instancia.

    Importante:
    - Esto NO es un histórico global del sistema, solo de ESTA Machine (porque la DB es local).
    """
    q = select(ManufacturedPiece).order_by(ManufacturedPiece.id.desc()).limit(limit).offset(offset)

    if order_id is not None:
        q = q.where(ManufacturedPiece.order_id == order_id)

    if result is not None:
        q = q.where(ManufacturedPiece.result == result)

    rows = (await db.execute(q)).scalars().all()
    return rows


@router.get("/pieces/{piece_id}", response_model=schemas.ManufacturedPieceOut)
async def get_piece_detail(
    piece_id: str,
    db: AsyncSession = Depends(get_db),
    user: int = Depends(get_current_user),
):
    """
    Devuelve una pieza concreta por piece_id.
    """
    q = select(ManufacturedPiece).where(ManufacturedPiece.piece_id == piece_id)
    row = (await db.execute(q)).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="piece_id not found")
    return row


@router.get("/orders", response_model=List[schemas.OrderSummaryOut])
async def list_orders_summary(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    user: int = Depends(get_current_user),
):
    """
    Devuelve un resumen por order_id: cuántas piezas fabricó esta instancia para cada orden.
    """
    q = (
        select(
            ManufacturedPiece.order_id.label("order_id"),
            func.count(ManufacturedPiece.id).label("manufactured_count"),
        )
        .where(ManufacturedPiece.result == "MANUFACTURED")
        .group_by(ManufacturedPiece.order_id)
        .order_by(func.count(ManufacturedPiece.id).desc())
        .limit(limit)
        .offset(offset)
    )

    rows = (await db.execute(q)).all()
    return [{"order_id": r.order_id, "manufactured_count": r.manufactured_count} for r in rows]


@router.get("/inflight", response_model=Optional[schemas.InflightPieceOut])
async def get_inflight(
    db: AsyncSession = Depends(get_db),
    user: int = Depends(get_current_user),
):
    """
    Muestra la pieza en curso persistida (si existe).

    Si la tabla está vacía, devuelve null.
    """
    q = select(InflightPiece).where(InflightPiece.id == 1)
    row = (await db.execute(q)).scalar_one_or_none()
    return row
