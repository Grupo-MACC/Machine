# -*- coding: utf-8 -*-
"""FastAPI router definitions."""
import logging
from fastapi import APIRouter, Depends
from business_logic.async_machine import Machine
from dependencies import get_machine
from microservice_chassis_grupo2.core.dependencies import get_current_user
from sql import schemas

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/machine"
)

@router.get(
    "/health",
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
        "/status"
)
async def get_machine_status(
    machine: Machine = Depends(get_machine),
    user: int = Depends(get_current_user)
):
    return machine.status