from enum import Enum as PyEnum
from xml.dom.minidom import Element
from sqlalchemy import inspect
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.exc import NoResultFound
from ..settings import the_settings
from ..utils import Hasher, Context, DefaultDict
from ..db.columntypes import Boolean, BigInteger, Enum, Integer, String, SmallInteger
from ..db.table import PrimaryKeyConstraint
from ..db.basemodel import intpk, BaseModel

MAX_CONFIGURATION_PART = 64
MAX_COMPONENT_NAME = 64
MAX_TABLE_NAME = 64
MAX_OPERATED_NAME = 64
MAX_SHARD_SUFFIX = 32

class Configuration(BaseModel):
    class CONF_TYPE(PyEnum): SCHEMA_PARAMS_0, SCHEMA_PARAMS_1 = 0, 1
    __tablename__ = 'sooners_configuration'
    __table_args__ = dict(table_priority = 'sooners.0001')
    id: Mapped[intpk]
    conf_type: Mapped[CONF_TYPE] = mapped_column(Enum(CONF_TYPE, name = 'sooners_conf_type'))
    conf_part_order: Mapped[int] = mapped_column(Integer())
    conf_part: Mapped[str] = mapped_column(String(MAX_CONFIGURATION_PART))
    @classmethod
    def load_configuration(cls, conf_type: CONF_TYPE, context: Context) -> str:
        context.default.inspector.clear_cache()
        if not context.default.inspector.has_table(cls.__tablename__): return None
        try:
            records = context.default.session.query(
                cls).filter_by(conf_type = conf_type).order_by('conf_part_order')
            return ''.join(map(lambda record: record.conf_part, records))
        except NoResultFound as exc: return None
    @classmethod
    def save_configuration(cls, conf_type: CONF_TYPE, conf: str | None, context: Context,
                           operate_ornot: bool = True, commit_ornot: bool = True) -> bool:
        context.default.inspector.clear_cache()
        if not context.default.inspector.has_table(cls.__tablename__): return False
        if conf is None: conf_parts = ()
        else: conf_parts = tuple(map(
                lambda start: conf[start: start + MAX_CONFIGURATION_PART],
                range(0, len(conf), MAX_CONFIGURATION_PART)))
        insert_record_dict, update_record_dict, delete_record_dict = dict(), dict(), dict()
        for record in context.default.session.query(cls).filter_by(conf_type = conf_type):
            if record.conf_part_order < len(conf_parts):
                update_record_dict[record.conf_part_order] = record
            else: delete_record_dict[record.conf_part_order] = record
        for conf_part_order, conf_part in enumerate(conf_parts):
            if conf_part_order in update_record_dict: continue
            insert_record_dict[conf_part_order] = cls(
                conf_type = conf_type,
                conf_part_order = conf_part_order,
                conf_part = conf_part)
        if operate_ornot:
            for record in insert_record_dict.values():
                context.default.session.add(record)
            for record in update_record_dict.values():
                record.body_part = conf_parts[record.conf_part_order]
            for record in delete_record_dict.values():
                context.default.session.delete(record)
        if commit_ornot: context.default.session.commit()
        return True

class _AutoVersion(object):
    def __init__(self, context: Context) -> None:
        self.session = context.default.session
        self.inspector = context.default.inspector
    def __call__(self, component_name: str):
        self.inspector.clear_cache()
        cndict = dict(component_name = component_name)
        if not self.inspector.has_table(DBSchemaVersion.__tablename__):
            return DBSchemaVersion(**cndict)
        try: return self.session.query(DBSchemaVersion).filter_by(**cndict).one()
        except NoResultFound as exc: return DBSchemaVersion(**cndict)

class DBSchemaVersion(BaseModel):
    @classmethod
    def load_default_dict(cls, context: Context) -> DefaultDict:
        version_records = DefaultDict(_AutoVersion(context))
        context.default.inspector.clear_cache()
        if not context.default.inspector.has_table(cls.__tablename__): pass
        else:
            for version_record in context.default.session.query(cls).all():
                version_records[version_record.component_name] = version_record
        return version_records
    @classmethod
    def save_default_dict(cls, context: Context, version_records: DefaultDict,
                          commit_ornot: bool = True) -> bool:
        context.default.inspector.clear_cache()
        if not context.default.inspector.has_table(cls.__tablename__): return False
        for version_record in version_records.values():
            if not inspect(version_record).transient: pass
            context.default.session.add(version_record)
        if commit_ornot: context.default.session.commit()
        return True
    __tablename__ = 'sooners_dbschema_version'
    __table_args__ = dict(table_priority = 'sooners.0002')
    component_name: Mapped[str] = mapped_column(String(MAX_COMPONENT_NAME), primary_key = True)
    index0: Mapped[int] = mapped_column(
        Integer(), default = 0) # component consequence at version0.
    version0: Mapped[int] = mapped_column(Integer(), nullable = True, default = None)
    checksum0: Mapped[str] = mapped_column(
        String(Hasher.checksum_size), nullable = True, default = None)
    index1: Mapped[int] = mapped_column(
        Integer(), default = 0) # component consequence at version1.
    version1: Mapped[int] = mapped_column(Integer(), nullable = True, default = None)
    checksum1: Mapped[str] = mapped_column(
        String(Hasher.checksum_size), nullable = True, default = None)
    def __repr__(self) -> str:
        return '%s(%r,%r,%r)' % (
            self.__class__.__name__, self.component_name, self.version0, self.version1)
    def is_same(self) -> bool:
        return self.version0 == self.version1 and self.checksum0 == self.checksum1
    def need_patch(self) -> bool:
        return None not in (self.version0, self.version1) and not self.is_same()

    def same0(self, context: Context) -> bool:
        return self.version0 == context.version and self.checksum0 == context.checksum
    def same1(self, context: Context) -> bool:
        return self.version1 == context.version and self.checksum1 == context.checksum

    def save0_none(self) -> None: self.index0, self.version0, self.checksum0 = 0, None, None
    def save1_none(self) -> None: self.index1, self.version1, self.checksum1 = 0, None, None

    def save0(self, index: int, context: Context) -> None:
        self.index0, self.version0, self.checksum0 = index, context.version, context.checksum
    def save1(self, index: int, context: Context) -> None:
        self.index1, self.version1, self.checksum1 = index, context.version, context.checksum

    def xmlversion0(self, settings) -> Element:
        if self.version0 is None: return None
        component = settings.components[self.component_name]
        xmlversion0 = component.version_parse(self.version0)
        if self.checksum0 is None: self.checksum0 = xmlversion0.getAttribute('checksum')
        else: assert(self.checksum0 == xmlversion0.getAttribute('checksum'))
        return xmlversion0
    def xmlversion1(self, settings) -> Element:
        if self.version1 is None: return None
        component = settings.components[self.component_name]
        xmlversion1 = component.version_parse(self.version1)
        if self.checksum1 is None: self.checksum1 = xmlversion1.getAttribute('checksum')
        else: assert(self.checksum1 == xmlversion1.getAttribute('checksum'))
        return xmlversion1
    def xmlpatch(self, settings) -> Element:
        assert(self.need_patch())
        component = settings.components[self.component_name]
        if self.version0 < self.version1:
            return component.patch_parse(self.version0, self.version1)
        elif self.version0 > self.version1:
            return component.patch_parse(self.version1, self.version0)
        else: assert(0)

class DBSchemaOperation(BaseModel):
    @classmethod
    def new_by_operation(cls, component_name: str, operation):
        names = operation.names()
        return cls(component_name = component_name, typeid = operation.typeid,
                   table = operation.table_name(), name0 = names[0], name1 = names[1])
    __tablename__ = 'sooners_dbschema_operation'
    __table_args__ = dict(table_priority = 'sooners.0003')
    __database_name_patterns__ = ('*',)
    id: Mapped[intpk]
    component_name: Mapped[str] = mapped_column(String(MAX_COMPONENT_NAME))
    typeid: Mapped[int] = mapped_column(Integer())
    table: Mapped[str] = mapped_column(String(MAX_TABLE_NAME), nullable = True)
    name0: Mapped[str] = mapped_column(String(MAX_OPERATED_NAME), nullable = True)
    name1: Mapped[str] = mapped_column(String(MAX_OPERATED_NAME), nullable = True)
    def key(self): return (self.typeid, self.table, self.name0, self.name1)
    def __repr__(self) -> str:
        from ..db.operations import OperationMeta
        if self.table is None: last_part = '%s->%s' % (self.name0, self.name1)
        else: last_part = '%s(%s->%s)' % (self.table, self.name0, self.name1)
        return '%s(%s@%s,%s)' % (
            self.__class__.__name__,
            OperationMeta.typeid2class[self.typeid].oper_member,
            self.component_name, last_part)

class ShardWeight(BaseModel):
    __tablename__ = 'sooners_shard_weight'
    __table_args__ = (
        PrimaryKeyConstraint('name', 'suffix', name = 'shard_weight_pk'),
        dict(table_priority = 'sooners.0004'))
    name: Mapped[str] = mapped_column(String(MAX_OPERATED_NAME))
    suffix: Mapped[str] = mapped_column(String(MAX_SHARD_SUFFIX))
    count: Mapped[int] = mapped_column(BigInteger())
    def __repr__(self) -> str:
        return '%s(%s_%s:%u)' % (self.__class__.__name__, self.name, self.suffix, self.count)
    def weight(self, max_count: int) -> int:
        return (max_count - self.count) + 1
