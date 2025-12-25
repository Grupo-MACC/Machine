# app_machine/business_logic/async_machine.py
# -*- coding: utf-8 -*-
"""
Lógica de fabricación (simulada) para Machine.

Diseño actual (según tu arquitectura):
- Machine NO consulta a Order.
- Machine fabrica exactamente lo que Warehouse le manda.
- Fabricación: sleep aleatorio (simulación).
- Devuelve un evento listo para publicar en `piece.done`.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from random import randint
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    """Devuelve fecha/hora actual UTC en ISO8601 con sufijo Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Machine:
    """
    Máquina de fabricación (simulada).

    Notas:
    - Se usa un Lock para asegurar 1 fabricación a la vez POR INSTANCIA.
      (Y RabbitMQ reparte entre instancias con prefetch=1).
    - `status` y `working_piece` permiten exponer estado por endpoint.
    """

    STATUS_WAITING = "Waiting"
    STATUS_WORKING = "Working"

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.status = self.STATUS_WAITING
        self.working_piece: Optional[Dict[str, Any]] = None

        self._min_s = int(os.getenv("MACHINE_MIN_SECONDS", "5"))
        self._max_s = int(os.getenv("MACHINE_MAX_SECONDS", "20"))

    @classmethod
    async def create(cls) -> "Machine":
        """Constructor asíncrono para mantener compatibilidad con tu injector."""
        logger.info("AsyncMachine initialized")
        return cls()

    async def manufacture_piece(self, order_id: int, piece_id: str, piece_type: str, order_date: Optional[str] = None) -> dict:
        """
        Fabrica una pieza.

        Args:
            order_id: Identificador de la orden.
            piece_id: UUID de la pieza (string).
            piece_type: Tipo de pieza ('A' o 'B').
            order_date: ISO date de la orden (de momento no se usa; futuro DB).

        Returns:
            dict listo para publicar en `piece.done`.
        """
        async with self._lock:
            self.status = self.STATUS_WORKING
            self.working_piece = {
                "order_id": order_id,
                "piece_id": piece_id,
                "piece_type": piece_type,
                "order_date": order_date,
            }

            seconds = randint(self._min_s, self._max_s)
            logger.info("[MACHINE-%s] 🛠️ Fabricando piece=%s order=%s (t=%ss)", piece_type, piece_id, order_id, seconds)
            await asyncio.sleep(seconds)

            done_event = {
                "order_id": order_id,
                "piece_id": piece_id,
                "piece_type": piece_type,
                "manufacturing_date": _utc_now_iso(),
            }

            self.status = self.STATUS_WAITING
            self.working_piece = None
            return done_event
