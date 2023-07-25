from dataclasses import make_dataclass
from fnmatch import fnmatch
from random import random
from typing import Annotated, Iterable
from sqlalchemy.ext.mutable import MutableComposite
from sqlalchemy.orm import DeclarativeBase, mapped_column, composite, Mapped
from ..utils import Context
from ..settings import the_settings
from .database import BaseDatabase
from .columntypes import Integer, String
from .table import Table, ShardTable

intpk = Annotated[int, mapped_column(
    Integer(), primary_key = True, autoincrement = True)]
#    init = False, default = 0, primary_key = True, autoincrement = True)]

class BaseShardOperator(MutableComposite):
    def ready(self) -> bool: return self.suffix is not None
    def choose(self, context: Context):
        if self.ready(): return self
        from ..core.models import ShardWeight
        shard_model = context.settings.metadata.models.shard[self.shard_model_name]
        # get weight_records.
        func0 = lambda weight_record: weight_record.suffix in shard_model.__suffix2model__
        func1 = lambda weight_record: (weight_record.suffix, weight_record)
        fdict = dict(name = self.shard_model_name)
        query = context.default.session.query(ShardWeight)
        weight_records = dict(map(func1, filter(func0, query.filter_by(**fdict))))
        # get free_weight_records.
        func0 = lambda suffix: suffix not in weight_records
        func1 = lambda suffix: (suffix, ShardWeight(
            name = self.shard_model_name, suffix = suffix, count = 0))
        suffixes = shard_model.__suffix2model__.keys()
        free_weight_records = dict(map(func1, filter(func0, suffixes)))
        # save new free_weight_records when avaiable.
        if free_weight_records:
            func = lambda free_weight_record: context.default.session.add(free_weight_record)
            tuple(map(func, free_weight_records.values()))
            context.default.session.commit()
        # add exists free weight record from weight_records to free_weight_records.
        func0 = lambda weight_record: weight_record.count == 0
        for weight_record in filter(func0, weight_records.values()):
            free_weight_records[weight_record.suffix] = weight_record
        # choose a free_weight_records when avaiable.
        if free_weight_records:
            free_suffixes = tuple(free_weight_records.keys())
            self.suffix = free_suffixes[int(random() * len(free_suffixes))]
            self.changed()
            return self
        # cut off free weight records from weight_records.
        func0 = lambda weight_record: weight_record.count > 0
        func1 = lambda weight_record: (weight_record.suffix, weight_record)
        weight_records = dict(map(func1, filter(func0, weight_records.values())))
        if not weight_records:
            raise RuntimeError('Not any available ShardWeight record for %r.' %
                               self.shard_model_name)
        # choose from weight_record by reminder of weight.
        func = lambda weight_record: weight_record.count
        max_count = max(map(func, weight_records.values()))
        func = lambda weight_record: (weight_record.weight(max_count), weight_record)
        weight_dict = dict(map(func, weight_records.values()))
        weight_at = int(random() * sum(weight_dict.keys()))
        for weight, weight_record in weight_dict.items():
            if weight < weight_at: weight_at -= weight
            else:
                self.suffix = weight_record.suffix
                self.changed()
                break
        return self
    def get_entity_model(self, context: Context):
        assert(self.suffix is not None)
        shard_model = context.settings.metadata.models.shard[self.shard_model_name]
        entity_model = shard_model.__suffix2model__[self.suffix]
        assert(len(entity_model.__database_names__) == 1)
        return entity_model
    def get_session(self, context: Context):
        entity_model = self.get_entity_model(context)
        database_name = tuple(entity_model.__database_names__)[0]
        return context.sessions[database_name]
    def new_one(self, context: Context, **fields):
        return self.get_entity_model(context)(**fields)
    def query(self, context: Context):
        entity_model = self.get_entity_model(context)
        database_name = tuple(entity_model.__database_names__)[0]
        return context.sessions[database_name].query(entity_model)

class DeclarativeMeta(DeclarativeBase.__class__):
    def __new__(metaclass, classname: str, bases: tuple[type],
                classdict: dict[str, object], **kwargs) -> type:
        func = lambda base: callable(getattr(base, 'pre_setup', None))
        for base in filter(func, bases): classdict = base.pre_setup(classname, classdict)
        cls = super().__new__(metaclass, classname, bases, classdict, **kwargs)
        return cls.post_setup(classdict, **kwargs)

class ShardOperatorMap(dict):
    def __getitem__(self, shard_model_name: str) -> BaseShardOperator:
        if shard_model_name not in self:
            shard_model_operator_class = make_dataclass(
                '%sOperator' % shard_model_name,
                [('suffix', str | None)], bases = (BaseShardOperator,),
                namespace = dict(shard_model_name = shard_model_name))
            super().__setitem__(shard_model_name, shard_model_operator_class)
        return super().__getitem__(shard_model_name)
shard_operator_map = ShardOperatorMap()

class BaseModelMixin(object):
    @classmethod
    def pre_setup(basecls, classname: str, classdict: dict[str, object]) -> dict[str, object]:
        # for shard model operators.
        if '__shard_model_names__' in classdict and '__annotations__' in classdict:
            for shard_model_name in classdict['__shard_model_names__']:
                operator_name = '%s_operator' % shard_model_name.lower()
                composite_name = '%s_composite' % shard_model_name.lower()
                classdict[operator_name] = composite(mapped_column(composite_name, String(64)))
                mapped_annotation = Mapped[shard_operator_map[shard_model_name]]
                classdict['__annotations__'][operator_name] = mapped_annotation
        # for auto generated fields.
        def _auto(name: str, value: object) -> Iterable[tuple[str, object]]:
            if not callable(generate := getattr(value, 'generate', None)): yield (name, value)
            else:
                for subname, subvalue in generate(name):
                    for subsubname, subsubvalue in _auto(subname, subvalue):
                        yield (subsubname, subsubvalue)
        classdict0 = classdict.__class__()
        for name, value in classdict.items(): classdict0.update(**dict(_auto(name, value)))
        if '__abstract__' not in classdict0: classdict0['__abstract__'] = False
        return classdict0
    @classmethod
    def post_setup(cls, classdict: dict[str, object], **kwargs) -> type:
        from pathlib import Path
        from sys import modules as sysmodules
        cls_path = Path(sysmodules[cls.__module__].__file__).absolute()
        component = cls.metadata.settings.components.locate_component(cls_path)
        cls.__component__ = component
        if hasattr(cls, '__table__'):
            cls.__table__.__component__ = component
            if hasattr(cls, '__table_priority__'):
                cls.__table__.__table_priority__ = cls.__table_priority__
        return cls

class BaseModel(DeclarativeBase, BaseModelMixin, metaclass = DeclarativeMeta):
    registry, metadata = the_settings.registry, the_settings.metadata
    __table_cls__, __abstract__ = Table, True
    @classmethod
    def post_setup(cls, classdict: dict[str, object], **kwargs) -> type:
        super().post_setup(classdict, **kwargs)
        if not cls.__abstract__: cls.metadata.models.plain[cls.__name__] = cls
        else: cls.metadata.models.plain_abstract[cls.__name__] = cls
        return cls
    @classmethod
    def setup(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        if cls.__abstract__: return
        if hasattr(cls, '__database_names__'): return
        if not hasattr(cls, '__database_name_patterns__'):
            cls.__database_names__ = { the_settings.default_database_name }
        else:
            func = lambda database_name: any(map(
                lambda pattern: fnmatch(database_name, pattern),
                cls.__database_name_patterns__))
            cls.__database_names__ = set(filter(func, databases.keys()))

class BaseShardModel(DeclarativeBase, BaseModelMixin, metaclass = DeclarativeMeta):
    registry, metadata = the_settings.registry, the_settings.metadata
    __table_cls__, __abstract__, __shard_suffix__ = ShardTable, True, None
    @classmethod
    def post_setup(cls, classdict: dict[str, object], **kwargs) -> type:
        super().post_setup(classdict, **kwargs)
        if cls.__abstract__: # for abstract shard models.
            cls.metadata.models.shard_abstract[cls.__name__] = cls
        elif cls.__shard_suffix__ is None: # for shard models.
            cls.metadata.models.shard[cls.__name__] = cls
            cls.__suffix2model__ = dict()
            cls.__model2database__ = dict()
            cls.__suffix2database__ = dict()
        else: # for shard entity models.
            cls.metadata.models.shard_entity[cls.__name__] = cls
        return cls

    @classmethod
    def shard_modelname(cls, shard_suffix: str) -> str:
        return '%s_%s' % (cls.__name__, shard_suffix)
    @classmethod
    def shard_tablename(cls, shard_suffix: str) -> str:
        return '%s_%s' % (cls.__tablename__, shard_suffix)
    @classmethod
    def create_entity_model(cls, database: BaseDatabase, shard_suffix: str) -> None:
        if shard_suffix in set(cls.__suffix2model__.keys()):
            excfmt = 'Shard suffix conflict detected for %r at %r.'
            raise ValueError(excfmt % (cls, shard_suffix))
        shard_table = cls.__table__.to_metadata(
            cls.metadata, name = cls.shard_tablename(shard_suffix))
        shard_table.__class__ = ShardTable
        shard_table.__shard_name__ = cls.__tablename__
        shard_table.__shard_suffix__ = shard_suffix
        if hasattr(cls, '__table_priority__'):
            shard_table.__table_priority__ = cls.__table_priority__
        shard_entity_model = type(
            cls.shard_modelname(shard_suffix), cls.__bases__, dict(
                __shard_name__ = cls.__name__, __shard_suffix__ = shard_suffix,
                __table__ = shard_table))
        cls.__suffix2model__[shard_suffix] = shard_entity_model
        cls.__model2database__[shard_entity_model] = database
        cls.__suffix2database__[shard_suffix] = database
    @classmethod
    def setup(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        if cls.__abstract__: return
        if cls.__shard_suffix__ is None: cls.setup_shard(params, databases)
        else: cls.setup_shard_entity(params, databases)
    @classmethod
    def setup_shard(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        if cls.__name__ not in params.__names__: return
        for dbname, suffixes in getattr(params, cls.__name__).shard_map.items():
            for shard_suffix in suffixes:
                cls.create_entity_model(databases[dbname], shard_suffix)
    @classmethod
    def setup_shard_entity(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        shard_map = getattr(params, cls.__shard_name__).shard_map
        func0 = lambda dbname2suffixes: cls.__shard_suffix__ in dbname2suffixes[1]
        func1 = lambda dbname2suffixes: dbname2suffixes[0]
        cls.__database_names__ = set(map(func1, filter(func0, shard_map.items())))
