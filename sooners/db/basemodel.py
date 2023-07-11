from fnmatch import fnmatch
from typing import Annotated, Iterable
from sqlalchemy.orm import DeclarativeBase, mapped_column
from ..utils import Context
from ..settings import the_settings
from .database import BaseDatabase
from .columntypes import Integer
from .table import Table, ShardTable

intpk = Annotated[int, mapped_column(
    Integer(), primary_key = True, autoincrement = True)]
#    init = False, default = 0, primary_key = True, autoincrement = True)]

class DeclarativeMeta(DeclarativeBase.__class__):
    def __new__(metaclass, classname: str, bases: tuple[type],
                classdict: dict[str, object], **kwargs) -> type:
        func = lambda base: callable(getattr(base, 'pre_setup', None))
        for base in filter(func, bases): classdict = base.pre_setup(classname, classdict)
        cls = super().__new__(metaclass, classname, bases, classdict, **kwargs)
        return cls.post_setup(classdict, **kwargs)

class BaseModelMixin(object):
    @classmethod
    def pre_setup(basecls, classname: str, classdict: dict[str, object]) -> dict[str, object]:
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
