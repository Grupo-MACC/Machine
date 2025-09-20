# -*- coding: utf-8 -*-
"""Application dependency injector."""
import logging

logger = logging.getLogger(__name__)

MY_MACHINE = None


# Machine #########################################################################################
async def get_machine():
    """Returns the machine object (creates it the first time its executed)."""
    logger.debug("Getting machine")
    global MY_MACHINE
    if MY_MACHINE is None:
        from business_logic.async_machine import Machine
        MY_MACHINE = await Machine.create()
    return MY_MACHINE


# asyncio.create_task(get_machine())
# asyncio.run(init_machine())
