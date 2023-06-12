from typing import Annotated
from sqlalchemy.orm import declared_attr, Mapped, mapped_column, relationship
from ..settings import the_settings
from ..db.table import ForeignKey
from ..db.columntypes import Integer, String
from ..db.basemodel import intpk, BaseModel, BaseBatchModel

name_column = Annotated[str, mapped_column(String(32))]
class Building(BaseModel):
    __tablename__ = 'sample1_building'
    id: Mapped[intpk]
    name: Mapped[name_column]
    floors: Mapped[list['Floor']] = relationship(
        back_populates = 'sample1_floor', cascade = 'all, delete-orphan')
building_id_column = Annotated[int, mapped_column(
    ForeignKey(Building.id, ondelete = 'CASCADE'))]

class Floor(BaseModel):
    __tablename__ = 'sample1_floor'
    id: Mapped[intpk]
    name: Mapped[name_column]
    building_id: Mapped[building_id_column]
    building: Mapped[Building] = relationship(back_populates = 'sample1_building')
floor_id_column = Annotated[int, mapped_column(
    ForeignKey(Floor.id, ondelete = 'CASCADE'))]

class Point(BaseBatchModel):
    __tablename__ = 'sample1_point'
    id: Mapped[intpk]
    name: Mapped[name_column]
    building_id: Mapped[building_id_column]
    floor_id: Mapped[floor_id_column]
