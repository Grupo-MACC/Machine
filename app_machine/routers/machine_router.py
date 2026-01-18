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
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from microservice_chassis_grupo2.core.dependencies import (
    get_current_user,
    get_db,
    check_public_key,
)

from sql.models import ManufacturedPiece, InflightPiece
from sql import schemas
from sql.blacklist_database import blacklist_session
from sql.blacklist_models import OrderBlacklistEntry
from business_logic.async_machine import Machine
from dependencies import get_machine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/machine")


@router.get("/health", include_in_schema=False)
async def health() -> dict:
    """ Healthcheck LIVENESS (para Consul / balanceadores). """
    return {"detail": "OK"}


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
async def list_fabricated_pieces(
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
            func.count(ManufacturedPiece.id).label("fabricated_count"),
        )
        .where(ManufacturedPiece.result == "MANUFACTURED")
        .group_by(ManufacturedPiece.order_id)
        .order_by(func.count(ManufacturedPiece.id).desc())
        .limit(limit)
        .offset(offset)
    )

    rows = (await db.execute(q)).all()
    return [{"order_id": r.order_id, "fabricated_count": r.fabricated_count} for r in rows]


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

#region blacklist delete
async def require_blacklist_admin(user: int = Depends(get_current_user)) -> int:
    """Permite endpoints de mantenimiento solo a admin (user_id=1)."""
    if user != 1:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized")
    return user

@router.get("/blacklist", response_model=schemas.BlacklistOut)
async def list_blacklist(
    limit: int = Query(default=5000, ge=1, le=20000, description="Máximo de order_id a devolver"),
    offset: int = Query(default=0, ge=0, description="Offset de paginación"),
    _: int = Depends(require_blacklist_admin),
):
    """
    Devuelve los order_id almacenados en la blacklist compartida.

    Importante:
        - Consulta la BD compartida 'blacklist-db' (no la DB local de cada Machine).
        - Se recomienda paginar (limit/offset) si la tabla crece.

    Seguridad:
        - Endpoint restringido a admin (user_id=1).
    """
    async with blacklist_session() as session:
        # Total de entradas
        total_q = select(func.count()).select_from(OrderBlacklistEntry)
        total = int((await session.execute(total_q)).scalar_one())

        # Lista de IDs (paginada)
        ids_q = (
            select(OrderBlacklistEntry.order_id)
            .order_by(OrderBlacklistEntry.order_id.asc())
            .limit(limit)
            .offset(offset)
        )
        rows = (await session.execute(ids_q)).all()
        order_ids = [r[0] for r in rows]

    return {"total": total, "order_ids": order_ids}


@router.delete("/blacklist", response_model=schemas.Message)
async def clear_blacklist_db(
    _: int = Depends(require_blacklist_admin),
):
    """Vacía por completo la tabla de blacklist compartida.

    ⚠️ PELIGRO:
        - Esto rompe la idempotencia de cancelación.
        - Solo debería usarse en entorno DEV/TEST.

    Devuelve:
        - Mensaje con el número de filas borradas.
    """
    async with blacklist_session() as session:
        result = await session.execute(delete(OrderBlacklistEntry))
        await session.commit()

    deleted = int(getattr(result, "rowcount", 0) or 0)
    return {"detail": f"Blacklist cleared. Deleted rows: {deleted}"}


@router.delete("/blacklist/{order_id}", response_model=schemas.Message)
async def delete_blacklist_entry(
    order_id: int,
    _: int = Depends(require_blacklist_admin),
):
    """Elimina un registro concreto (order_id) de la blacklist compartida.

    Uso típico:
        - Reset de tests.
        - Corrección manual en DEV.

    Errores:
        - 404 si no existe ese order_id en la blacklist.
    """
    async with blacklist_session() as session:
        result = await session.execute(
            delete(OrderBlacklistEntry).where(OrderBlacklistEntry.order_id == order_id)
        )
        await session.commit()

    deleted = int(getattr(result, "rowcount", 0) or 0)
    if deleted == 0:
        raise HTTPException(status_code=404, detail="order_id not found in blacklist")
    return {"detail": f"Deleted blacklist entry for order_id={order_id}"}