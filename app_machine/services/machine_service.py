import logging
import httpx
from typing import List
from dependencies import get_machine
from sql import schemas
from consul_client import get_service_url

logger = logging.getLogger(__name__)

async def add_pieces_to_queue(
    pieces: List[str],
):
    try:
        pieces_obj = []
        machine = await get_machine()
        
        # Discover order service via Consul
        order_service_url = await get_service_url("order", "https://order:5000")
        
        async with httpx.AsyncClient(
                    verify="/certs/ca.pem",
                    cert=("/certs/machine/machine-cert.pem", "/certs/machine/machine-key.pem"),
                    ) as client:
            for piece_id in pieces:
                try:
                    response = await client.get(f"{order_service_url}/private/piece/{piece_id}")
                    response.raise_for_status()
                    piece_data = response.json()

                    # Convertimos el JSON a objeto interno (schemas.Piece)
                    pieces_obj.append(schemas.Piece(**piece_data))

                except httpx.HTTPError as exc:
                    print(exc)
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