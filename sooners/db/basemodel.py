from fnmatch import fnmatch
from typing import Annotated, Iterable
from sqlalchemy.orm import DeclarativeBase, mapped_column
from ..utils import Context
from ..settings import the_settings
from .database import BaseDatabase
from .columntypes import Integer
from .table import Table, BatchTable

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
        if hasattr(cls, '__table__'): cls.__table__.__component__ = component
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

class BaseBatchModel(DeclarativeBase, BaseModelMixin, metaclass = DeclarativeMeta):
    registry, metadata = the_settings.registry, the_settings.metadata
    __table_cls__, __abstract__, __batch_suffix__ = BatchTable, True, None
    @classmethod
    def post_setup(cls, classdict: dict[str, object], **kwargs) -> type:
        super().post_setup(classdict, **kwargs)
        if cls.__abstract__: # for abstract batch models.
            cls.metadata.models.batch_abstract[cls.__name__] = cls
        elif cls.__batch_suffix__ is None: # for batch models.
            cls.metadata.models.batch[cls.__name__] = cls
            cls.__suffix2model__ = dict()
            cls.__model2database__ = dict()
            cls.__suffix2database__ = dict()
        else: # for batch entity models.
            cls.metadata.models.batch_entity[cls.__name__] = cls
        return cls

    @classmethod
    def batch_modelname(cls, batch_suffix: str) -> str:
        return '%s_%s' % (cls.__name__, batch_suffix)
    @classmethod
    def batch_tablename(cls, batch_suffix: str) -> str:
        return '%s_%s' % (cls.__tablename__, batch_suffix)
    @classmethod
    def create_entity_model(cls, database: BaseDatabase, batch_suffix: str) -> None:
        if batch_suffix in set(cls.__suffix2model__.keys()):
            excfmt = 'Batch suffix conflict detected for %r at %r.'
            raise ValueError(excfmt % (cls, batch_suffix))
        batch_table = cls.__table__.to_metadata(
            cls.metadata, name = cls.batch_tablename(batch_suffix))
        batch_table.__class__ = BatchTable
        batch_table.__batch_name__ = cls.__tablename__
        batch_table.__batch_suffix__ = batch_suffix
        batch_entity_model = type(
            cls.batch_modelname(batch_suffix), cls.__bases__, dict(
                __batch_name__ = cls.__name__, __batch_suffix__ = batch_suffix,
                __table__ = batch_table))
        cls.__suffix2model__[batch_suffix] = batch_entity_model
        cls.__model2database__[batch_entity_model] = database
        cls.__suffix2database__[batch_suffix] = database
    @classmethod
    def setup(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        if cls.__abstract__: return
        if cls.__batch_suffix__ is None: cls.setup_batch(params, databases)
        else: cls.setup_batch_entity(params, databases)
    @classmethod
    def setup_batch(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        if cls.__name__ not in params.__names__: return
        for dbname, suffixes in getattr(params, cls.__name__).batch_map.items():
            for batch_suffix in suffixes:
                cls.create_entity_model(databases[dbname], batch_suffix)
    @classmethod
    def setup_batch_entity(cls, params: Context, databases: dict[str, BaseDatabase]) -> None:
        batch_map = getattr(params, cls.__batch_name__).batch_map
        func0 = lambda dbname2suffixes: cls.__batch_suffix__ in dbname2suffixes[1]
        func1 = lambda dbname2suffixes: dbname2suffixes[0]
        cls.__database_names__ = set(map(func1, filter(func0, batch_map.items())))
