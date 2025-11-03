# -*- coding: utf-8 -*-
"""Database models definitions. Table representations as class."""
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey
from microservice_chassis_grupo2.sql.models import BaseModel

class Piece(BaseModel):
    """Piece database table representation."""
    STATUS_CREATED = "Created"
    STATUS_CANCELLED = "Cancelled"
    STATUS_QUEUED = "Queued"
    STATUS_MANUFACTURING = "Manufacturing"
    STATUS_MANUFACTURED = "Manufactured"

    __tablename__ = "piece"
    id = Column(Integer, primary_key=True)
    manufacturing_date = Column(DateTime(timezone=True), server_default=None)
    status = Column(String(256), default=STATUS_QUEUED)
    order_id = Column(
        Integer,
        ForeignKey('manufacturing_order.id', ondelete='cascade'),
        nullable=True)