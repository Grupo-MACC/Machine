# -*- coding: utf-8 -*-
"""Simulation of a machine that manufactures pieces."""
import asyncio
import logging
import httpx
from random import randint
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import ProgrammingError, OperationalError
from sql.models import Piece, Order
from routers.router_utils import ORDER_SERVICE_URL

logger = logging.getLogger(__name__)
logger.debug("Machine logger set.")


class Machine:
    """Piece manufacturing machine simulator."""
    STATUS_WAITING = "Waiting"
    STATUS_CHANGING_PIECE = "Changing Piece"
    STATUS_WORKING = "Working"
    __manufacturing_queue = asyncio.Queue()
    __stop_machine = False
    working_piece = None
    status = STATUS_WAITING

    @classmethod
    async def create(cls):
        """Machine constructor: loads manufacturing/queued pieces and starts simulation."""
        logger.info("AsyncMachine initialized")
        self = Machine()
        asyncio.create_task(self.manufacturing_coroutine())
        await self.reload_queue_from_database()
        return self

    async def reload_queue_from_database(self):
        """Reload queue from database, to reload data when the system has been rebooted."""
        # Load the piece that was being manufactured
        manufacturing_piece = await Machine.get_manufacturing_piece()
        if manufacturing_piece:
            await self.add_piece_to_queue(manufacturing_piece)

        # Load the pieces that were in the queue
        queued_pieces = await Machine.get_queued_pieces()

        if queued_pieces:
            await self.add_pieces_to_queue(queued_pieces)

    @staticmethod
    async def get_manufacturing_piece():
        """Gets the manufacturing piece from the database."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{ORDER_SERVICE_URL}/piece_status/{Piece.STATUS_MANUFACTURING}"
                )
                response.raise_for_status()
                manufacturing_pieces = response.json()
            if manufacturing_pieces and manufacturing_pieces[0]:
                return manufacturing_pieces[0]
        except (ProgrammingError, OperationalError):
            logger.error(
                "Error getting Manufacturing Piece at startup. It may be the first execution"
            )
        except httpx.AsyncClient as exc:
            logger.error("")
        return None

    @staticmethod
    async def get_queued_pieces():
        """Get all queued pieces from the database."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{ORDER_SERVICE_URL}/piece_status/{Piece.STATUS_QUEUED}"
                )
                response.raise_for_status()
                queued_pieces = response.json()
                return queued_pieces
        except (ProgrammingError, OperationalError):
            logger.error("Error getting Queued Pieces at startup. It may be the first execution")
            return []
        except httpx.AsyncClient as exc:
            return []

    async def manufacturing_coroutine(self) -> None:
        """Coroutine that manufactures queued pieces one by one."""
        while not self.__stop_machine:
            if self.__manufacturing_queue.empty():
                self.status = self.STATUS_WAITING
            piece_id = await self.__manufacturing_queue.get()
            await self.create_piece(piece_id)
            self.__manufacturing_queue.task_done()

    async def create_piece(self, piece_id: int):
        """Simulates piece manufacturing."""
        # Machine and piece status updated during manufacturing
        print("aaaaaaaaaaaaaaa")
        await self.update_working_piece(piece_id)
        await self.working_piece_to_manufacturing()  # Update Machine&piece status

        await asyncio.sleep(randint(5, 20))  # Simulates time spent manufacturing

        await self.working_piece_to_finished()  # Update Machine&Piece status

        self.working_piece = None

    async def update_working_piece(self, piece_id: int):
        """Loads a piece for the given id and updates the working piece."""
        logger.debug("Updating working piece to %i", piece_id)
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{ORDER_SERVICE_URL}/piece/{piece_id}"
                )
                response.raise_for_status()
                data = response.json()
                piece = Piece(
                    id=data["id"],
                    manufacturing_date=data["manufacturing_date"],
                    status=data["status"],
                    order_id=data["order"]["id"] if data.get("order") else None,
)
                self.working_piece = piece.as_dict()
                print(self.working_piece)
        except httpx.HTTPError as exc:
            print(exc)
        except Exception as exc:
            print(exc)

    async def working_piece_to_manufacturing(self):
        """Updates piece status to manufacturing."""
        self.status = Machine.STATUS_WORKING
        try:
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{ORDER_SERVICE_URL}/update_piece_status/{self.working_piece['id']}",
                    json=Piece.STATUS_MANUFACTURING
                )
                response.raise_for_status()
                answer = response.json()
                if answer:
                    print("Piece %i status updated to manufacturing via service.", self.working_piece['id'])
        except httpx.HTTPError as exc:
            print("Could not update working piece status to manufacturing: %s", exc) 
        except Exception as exc:
            print(exc)

    async def working_piece_to_finished(self):
        """Updates piece status to finished and order if all pieces are finished."""
        logger.debug("Working piece finished.")
        self.status = Machine.STATUS_CHANGING_PIECE

        try:
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{ORDER_SERVICE_URL}/update_piece_status/{self.working_piece['id']}",
                    json=Piece.STATUS_MANUFACTURED
                )
                response.raise_for_status()
                data = response.json()
                piece = Piece(
                    id=data["id"],
                    manufacturing_date=data["manufacturing_date"],
                    status=data["status"],
                    order_id=data["order"]["id"] if data.get("order") else None,
)
                if piece:
                    self.working_piece = piece.as_dict()
                    logger.info("Piece %i status updated to manufactured via service.", self.working_piece['id'])
        except httpx.HTTPError as exc:
            logger.error("Could not update working piece status to manufactured: %s", exc) 
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{ORDER_SERVICE_URL}/update_piece_manufacturing_date_to_now/{self.working_piece['id']}"
                )
                response.raise_for_status()
                data = response.json()
                piece = Piece(
                    id=data["id"],
                    manufacturing_date=data["manufacturing_date"],
                    status=data["status"],
                    order_id=data["order"]["id"] if data.get("order") else None,
)
                if piece:
                    self.working_piece = piece.as_dict()
        except httpx.HTTPError as exc:
            print(exc)
        except Exception as exc:
            print(exc)

        if await Machine.is_order_finished(self.working_piece['order_id']):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.put(
                        f"{ORDER_SERVICE_URL}/update_order_status/{self.working_piece['order_id']}",
                        json={"status": Order.STATUS_FINISHED}
                    )
            except httpx.HTTPError as exc:
                print(exc)
            except Exception as exc:
                print(exc)

    @staticmethod
    async def is_order_finished(order_id):
        """Return whether an order is finished or not."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{ORDER_SERVICE_URL}/order/{order_id}"
                )
                response.raise_for_status()
                db_order = response.json()
                '''db_order = Order(
                    id = data["id"],
                    number_of_pieces = data["number_of_pieces"]
                )'''
                pieces = db_order["pieces"]

        except httpx.HTTPError as exc:
            logger.error("")
        if not db_order:  # Just in case order has been removed
            return False
        
        try:
            #for piece in db_order.pieces:
            for piece in pieces:
                if piece["status"] != Piece.STATUS_MANUFACTURED:
                    return False
        except Exception as exc:
            print(exc)
        return True

    async def add_pieces_to_queue(self, pieces):
        """Adds a list of pieces to the queue and updates their status."""
        logger.debug("Adding %i pieces to queue", len(pieces))
        for piece in pieces:
            await self.add_piece_to_queue(piece)

    async def add_piece_to_queue(self, piece):
        """Adds the given piece to the queue."""
        try:
            # Asegúrate de que piece es un objeto con atributo id
            piece_id = getattr(piece, 'id', None) or (piece.get('id') if isinstance(piece, dict) else None)
            if piece_id is None:
                raise ValueError(f"Piece {piece} has neither attribute nor key 'id'")
            
            await self.__manufacturing_queue.put(piece_id)
            logger.debug("After adding piece %s to queue", piece_id)
            logger.debug("Piece %s added to manufacturing queue", piece_id)
            
        except Exception as e:
            print("Failed to add piece to queue:", piece, e)
            logger.exception("Failed to add piece %s to queue: %s", piece, e)



    async def remove_pieces_from_queue(self, pieces):
        """Adds a list of pieces to the queue and updates their status."""
        logger.debug("Removing %i pieces from queue", len(pieces))
        for piece in pieces:
            await self.remove_piece_from_queue(piece)

    async def remove_piece_from_queue(self, piece) -> bool:
        """Removes the given piece from the queue."""
        logger.info("Removing piece %i", piece)
        if self.working_piece == piece:
            logger.warning(
                "Piece %i is being manufactured, cannot remove from queue\n\n",
                piece
            )
            return False

        item_list = []
        removed = False
        # Empty the list
        while not self.__manufacturing_queue.empty():
            item_list.append(self.__manufacturing_queue.get_nowait())

        # Fill the list with all items but *piece_id*
        for item in item_list:
            if item != piece:
                self.__manufacturing_queue.put_nowait(item)
            else:
                logging.debug("Piece %i removed from queue.", piece)
                removed = True

        if not removed:
            logger.warning("Piece %i not found in the queue.", piece)

        return removed

    async def list_queued_pieces(self):
        """Get queued piece ids as list."""
        piece_list = list(self.__manufacturing_queue.__dict__['_queue'])
        return piece_list
