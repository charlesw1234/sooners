from typing import Annotated
from sqlalchemy.orm import declared_attr, Mapped, mapped_column, relationship
from ..settings import the_settings
from ..db.table import ForeignKey
from ..db.columntypes import Integer, String
from ..db.basemodel import intpk, BaseModel, BaseShardModel

name_column = Annotated[str, mapped_column(String(32))]
class Building(BaseModel):
    __tablename__ = 'sample1_building'
    id: Mapped[intpk]
    name: Mapped[name_column]
    floors: Mapped[list['Floor']] = relationship(
        back_populates = 'building', cascade = 'all, delete-orphan')

class Floor(BaseModel):
    __tablename__ = 'sample1_floor'
    id: Mapped[intpk]
    name: Mapped[name_column]
    building_id: Mapped[int] = mapped_column(ForeignKey(Building.id, ondelete = 'CASCADE'))
    building: Mapped[Building] = relationship(back_populates = 'floors')

class Point(BaseShardModel):
    __tablename__ = 'sample1_point'
    id: Mapped[intpk]
    name: Mapped[name_column]
    building_id: Mapped[int]
    floor_id: Mapped[int]
    point_type: Mapped[int] = mapped_column(nullable = True)
