# app_machine/business_logic/async_machine.py
# -*- coding: utf-8 -*-
"""
Lógica de fabricación con persistencia e idempotencia.

Incluye:
- Guardado de pieza en curso (inflight_piece) para reanudar tras reinicio.
- Guardado de piezas finalizadas (fabricated_piece).
- Blacklist local (order_blacklist) para saltar órdenes canceladas.
- Idempotencia por piece_id: si ya está registrada como procesada, no se refabrica.

NOTA IMPORTANTE (adaptación DB):
- Antes se usaba SessionLocal desde sql.database.
- Ahora la sesión se obtiene mediante get_db del chassis.
- get_db es un async generator, por eso lo envolvemos en un async context manager
  para abrir/cerrar sesión correctamente.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from random import randint

from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError

from microservice_chassis_grupo2.core.dependencies import get_db
from sql.models import ManufacturedPiece, InflightPiece
from sql.blacklist_database import blacklist_session
from sql.blacklist_models import OrderBlacklistEntry
logger = logging.getLogger(__name__)

#region 0. HELPERS
@asynccontextmanager
async def db_session():
    """
    Context manager para obtener una AsyncSession usando get_db().

    get_db() (del chassis) es un async generator (yield session).
    Para usarlo fuera de Depends(...), lo convertimos a context manager.

    Esto evita:
    - fugas de conexiones/sesiones
    - dejar el generador sin cerrar
    """
    agen = get_db()
    try:
        session = await anext(agen)  # Python 3.12+
        yield session
    finally:
        await agen.aclose()


def parse_iso_to_dt(value: str | None) -> datetime | None:
    """
    Convierte ISO string a datetime aware.

    Acepta formato con 'Z' al final (lo convierte a +00:00).
    """
    if not value:
        return None
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    return datetime.fromisoformat(v)


def utc_now_iso() -> str:
    """Devuelve datetime UTC en ISO con Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


#region 1. MACHINE
class Machine:
    """
    Máquina de fabricación con DB local.

    Variables de entorno útiles:
    - MACHINE_MIN_SECONDS / MACHINE_MAX_SECONDS: rango de tiempo de fabricación.
    - MACHINE_RESUME_ON_BOOT: 'true'/'false' para reanudar inflight al arrancar.
    """

    STATUS_WAITING = "Waiting"
    STATUS_WORKING = "Working"

    def __init__(self) -> None:
        self.status = self.STATUS_WAITING
        self.working_piece = None

        self._min_s = int(os.getenv("MACHINE_MIN_SECONDS", "5"))
        self._max_s = int(os.getenv("MACHINE_MAX_SECONDS", "20"))
        self._resume_on_boot = os.getenv("MACHINE_RESUME_ON_BOOT", "true").lower() == "true"

        self._lock = asyncio.Lock()

    @classmethod
    async def create(cls) -> "Machine":
        """Constructor asíncrono."""
        logger.info("AsyncMachine initialized")
        return cls()

    #region 1.1 blacklist
    async def is_order_blacklisted(self, order_id: int) -> bool:
        """
        True si el order_id está en la blacklist compartida.

        Implementación:
            - Consulta BD compartida (Postgres/Aurora).
            - PK = order_id => lookup directo.
        """
        async with blacklist_session() as session:
            row = await session.get(OrderBlacklistEntry, order_id)
            return row is not None

    async def add_to_blacklist(self, order_id: int, reason: str | None = None) -> None:
        """
        Inserta (idempotente) order_id en blacklist compartida.

        Idempotencia:
            - PK order_id => si ya existe, no debe romper el flujo.
        """
        from sqlalchemy.exc import IntegrityError  # import local para evitar ruido global

        async with blacklist_session() as session:
            session.add(OrderBlacklistEntry(order_id=order_id, reason=reason))
            try:
                await session.commit()
            except IntegrityError:
                # Ya existía -> idempotente
                await session.rollback()

    async def remove_from_blacklist(self, order_id: int) -> None:
        """
        TODO (admin/ops):
            - En el flujo normal NO se elimina.
            - Si más adelante decides añadir mantenimiento admin, aquí iría el delete.
        """
        raise NotImplementedError("Blacklist compartida: no se elimina en el flujo normal.")

    #region 1.2 db pieces
    async def is_piece_already_processed(self, piece_id: str) -> bool:
        """True si esta instancia ya registró esa piece_id en fabricated_piece."""
        async with db_session() as session:
            q = select(ManufacturedPiece).where(ManufacturedPiece.piece_id == piece_id)
            row = (await session.execute(q)).scalar_one_or_none()
            return row is not None

    async def mark_done_published(self, piece_id: str) -> None:
        """Marca que ya se publicó el evento piece.done para esa piece_id."""
        async with db_session() as session:
            row = (await session.execute(
                select(ManufacturedPiece).where(ManufacturedPiece.piece_id == piece_id)
            )).scalar_one_or_none()
            if row:
                row.done_published = True
                await session.commit()

    #region 1.3 doing piece
    async def resume_inflight_if_any(self) -> dict | None:
        """
        Si hay una pieza en curso guardada, intenta terminarla.

        Devuelve:
            Evento piece.done a publicar, o None si no hay nada que reanudar.

        Limitación importante (realista):
        - Con varias réplicas, el mensaje puede haberse reentregado a otra instancia y fabricado ya.
          Por eso usamos piece_id UNIQUE e idempotencia: si ya está en fabricated_piece, no rehacemos.
        """
        if not self._resume_on_boot:
            return None

        async with db_session() as session:
            inflight = (await session.execute(
                select(InflightPiece).where(InflightPiece.id == 1)
            )).scalar_one_or_none()
            if not inflight:
                return None

            already = (await session.execute(
                select(ManufacturedPiece).where(ManufacturedPiece.piece_id == inflight.piece_id)
            )).scalar_one_or_none()
            if already:
                await session.execute(delete(InflightPiece).where(InflightPiece.id == 1))
                await session.commit()
                return None

            now = datetime.now(timezone.utc)
            elapsed = (now - inflight.started_at).total_seconds()
            remaining = max(0, inflight.duration_s - int(elapsed))

            logger.warning(
                "[MACHINE] 🔁 Reanudando inflight piece=%s (remaining=%ss)",
                inflight.piece_id, remaining
            )

            self.status = self.STATUS_WORKING
            self.working_piece = {
                "order_id": inflight.order_id,
                "piece_id": inflight.piece_id,
                "piece_type": inflight.piece_type,
                "order_date": inflight.order_date.isoformat() if inflight.order_date else None,
            }

        if remaining > 0:
            await asyncio.sleep(remaining)

        done_event = await self._finalize_piece(
            order_id=self.working_piece["order_id"],
            piece_id=self.working_piece["piece_id"],
            piece_type=self.working_piece["piece_type"],
            order_date=self.working_piece["order_date"],
            result="MANUFACTURED",
            reason=None,
            from_resume=True,
        )
        return done_event

    #region 1.4 main process
    async def fabricate_piece(self, order_id: int, piece_id: str, piece_type: str, order_date: str | None) -> dict | None:
        """
        Fabrica una pieza con persistencia.

        - Si order_id está en blacklist: no fabrica (y opcionalmente registra SKIPPED).
        - Si piece_id ya está registrada: no fabrica (idempotencia).
        - Guarda inflight antes de dormir (para resume).
        - Al terminar, guarda fabricated_piece con done_published=False.
          (El broker marcará done_published=True tras publicar en RabbitMQ).

        Devuelve:
            Evento piece.done a publicar, o None si se decide “no fabricar”.
        """
        async with self._lock:
            if await self.is_order_blacklisted(order_id):
                logger.info("[MACHINE] ⛔ Order %s en blacklist: skip piece %s", order_id, piece_id)
                await self._finalize_piece(
                    order_id=order_id,
                    piece_id=piece_id,
                    piece_type=piece_type,
                    order_date=order_date,
                    result="SKIPPED",
                    reason="ORDER_BLACKLISTED",
                    from_resume=False,
                )
                return None

            if await self.is_piece_already_processed(piece_id):
                logger.info("[MACHINE] ♻️ piece_id %s ya procesada: ACK sin refabricar", piece_id)
                return None

            duration = randint(self._min_s, self._max_s)
            started_at = datetime.now(timezone.utc)

            self.status = self.STATUS_WORKING
            self.working_piece = {"order_id": order_id, "piece_id": piece_id, "piece_type": piece_type, "order_date": order_date}

            async with db_session() as session:
                await session.execute(delete(InflightPiece).where(InflightPiece.id == 1))
                session.add(InflightPiece(
                    id=1,
                    order_id=order_id,
                    piece_id=piece_id,
                    piece_type=piece_type,
                    order_date=parse_iso_to_dt(order_date),
                    started_at=started_at,
                    duration_s=duration,
                    done_published=False,
                ))
                await session.commit()

            logger.info("[MACHINE-%s] 🛠️ Fabricando piece=%s order=%s (t=%ss)", piece_type, piece_id, order_id, duration)
            await asyncio.sleep(duration)

            if await self.is_order_blacklisted(order_id):
                logger.warning("[MACHINE] 🚫 Order %s se canceló DURANTE fabricación. Skip publish.", order_id)
                await self._finalize_piece(
                    order_id=order_id,
                    piece_id=piece_id,
                    piece_type=piece_type,
                    order_date=order_date,
                    result="SKIPPED",
                    reason="CANCELLED_DURING_MANUFACTURING",
                    from_resume=False,
                )
                return None

            done_event = await self._finalize_piece(
                order_id=order_id,
                piece_id=piece_id,
                piece_type=piece_type,
                order_date=order_date,
                result="MANUFACTURED",
                reason=None,
                from_resume=False,
            )
            return done_event

    #region 1.5 end piece
    async def _finalize_piece(
        self,
        order_id: int,
        piece_id: str,
        piece_type: str,
        order_date: str | None,
        result: str,
        reason: str | None,
        from_resume: bool,
    ) -> dict:
        """
        Persiste finalización (MANUFACTURED o SKIPPED), limpia inflight y devuelve el evento.

        Nota:
        - done_published se deja en False para que el broker lo marque a True tras publicar.
        """
        fabricated_at_dt = datetime.now(timezone.utc)
        order_date_dt = parse_iso_to_dt(order_date)

        async with db_session() as session:
            try:
                session.add(ManufacturedPiece(
                    piece_id=piece_id,
                    order_id=order_id,
                    piece_type=piece_type,
                    order_date=order_date_dt,
                    fabrication_date=(fabricated_at_dt if result == "MANUFACTURED" else None),
                    done_published=False,
                    result=result,
                    reason=reason,
                ))
                await session.commit()
            except IntegrityError:
                await session.rollback()

            await session.execute(delete(InflightPiece).where(InflightPiece.id == 1))
            await session.commit()

        self.status = self.STATUS_WAITING
        self.working_piece = None

        return {
            "order_id": order_id,
            "piece_id": piece_id,
            "piece_type": piece_type,
            "order_date": order_date,
            "fabrication_date": utc_now_iso(),
            "result": result,
            "reason": reason,
            "from_resume": from_resume,
        }
