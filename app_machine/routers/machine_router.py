# -*- coding: utf-8 -*-
"""FastAPI router definitions."""
import logging
import httpx
from typing import List
from fastapi import APIRouter, Depends, status, Body, Query
from sqlalchemy.ext.asyncio import AsyncSession
from business_logic.async_machine import Machine
from dependencies import get_machine
from sql import schemas
from .router_utils import raise_and_log_error, ORDER_SERVICE_URL

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get(
    "/",
    summary="Health check endpoint",
    response_model=schemas.Message,
)
async def health_check():
    """Endpoint to check if everything started correctly."""
    logger.debug("GET '/' endpoint called.")
    return {
        "detail": "OK"
    }

@router.get(
        "/machine/status"
)
async def get_machine_status(
    machine: Machine = Depends(get_machine)
):
    return machine.status

@router.post(
    "/add_pieces_to_queue",
    status_code=status.HTTP_201_CREATED,
    summary="Add pieces to machine queue",
    response_description="Pieces successfully added to the queue."
)
async def add_pieces_to_queue(
    pieces: List[str] = Body(...),
    machine: Machine = Depends(get_machine)
):
    pieces_obj = []
    print("aaaaaaa")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:  # Un solo cliente para todo el loop
            for piece_id in pieces:
                try:
                    response = await client.get(f"{ORDER_SERVICE_URL}/piece/{piece_id}")
                    response.raise_for_status()
                    piece_data = response.json()

                    # Convertimos el JSON a objeto interno (schemas.Piece)
                    pieces_obj.append(schemas.Piece(**piece_data))

                except httpx.HTTPError as exc:
                    logger.error(
                        "HTTP error fetching piece %d from Order service: %s", 
                        piece_id, exc
                    )
                except Exception as exc:
                    logger.exception(
                        "Unexpected error processing piece %d from Order service", 
                        piece_id
                    )

        # Añadimos todas las piezas a la cola de la máquina
        await machine.add_pieces_to_queue(pieces=pieces_obj)
        print("Successfully added %d piece(s) to machine queue.", len(pieces_obj))
        return True

    except Exception as e:
        logger.exception("Error while adding pieces to machine queue")
        return False


@router.delete(
    "/remove_pieces_from_queue",
    status_code=status.HTTP_200_OK,
    summary="Remove pieces from machine queue",
    response_description="Pieces successfully removed from the queue."
)
async def remove_pieces_from_queue(
    piece_ids: List[int] = Query(...),
    machine: Machine = Depends(get_machine)
):
    logger.info("Request received to remove %d piece(s) from machine queue.", len(piece_ids))

    try:
        await machine.remove_pieces_from_queue(pieces=piece_ids)
        return True
    except Exception as e:
        logger.error("Error while removing pieces from queue: %s", str(e))
        return False