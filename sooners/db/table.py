from typing import Iterable
from xml.dom.minidom import Element
from sqlalchemy import MetaData as SAMetaData, Table as SATable, Column as SAColumn
from sqlalchemy import Constraint as SAConstraint, Index as SAIndex
from sqlalchemy import PrimaryKeyConstraint as SAPrimaryKeyConstraint
from sqlalchemy import ForeignKeyConstraint as SAForeignKeyConstraint
from sqlalchemy import UniqueConstraint as SAUniqueConstraint
from sqlalchemy import CheckConstraint as SACheckConstraint
from sqlalchemy import ColumnDefault as SAColumnDefault
from sqlalchemy import ForeignKey as SAForeignKey
from ..utils import Context, Arguments
from .mixins import SA2SN, SNBaseMixin, SNVersionMixin, SNPatchMixin
from .columntypes import bool_parser, column_type_map, ColumnTypeMixin
from .operations import BaseOperation
from .operations import CreateTable, RenameTable, DropTable
from .operations import CreateColumn, AlterColumn, DropColumn
from .operations import CreatePrimaryKeyConstraint, DropPrimaryKeyConstraint
from .operations import CreateForeignKeyConstraint, DropForeignKeyConstraint
from .operations import CreateUniqueConstraint, DropUniqueConstraint
from .operations import CreateCheckConstraint, DropCheckConstraint
from .operations import CreateIndex, DropIndex

class ForeignKey(SAForeignKey):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element):
        kwargs = dict()
        column = xmlele.getAttribute('column')
        if xmlele.hasAttribute('name'): kwargs.update(name = xmlele.getAttribute('name'))
        if xmlele.hasAttribute('onupdate'):
            kwargs.update(onupdate = xmlele.getAttribute('onupdate'))
        if xmlele.hasAttribute('ondelete'):
            kwargs.update(ondelete = xmlele.getAttribute('ondelete'))
        return cls(column, **kwargs)

    def save_to_subeles(self, xmlele: Element) -> None:
        xmlele.appendChild(subxmlele := xmlele.ownerDocument.createElement('ForeignKey'))
        attrs = [('column', self.target_fullname)]
        if self.name is not None: attrs.append(('name', self.name))
        if self.onupdate is not None: attrs.append(('onupdate', self.onupdate))
        if self.ondelete is not None: attrs.append(('ondelete', self.ondelete))
        tuple(map(lambda attr: subxmlele.setAttribute(attr[0], attr[1]), attrs))
SA2SN.register(ForeignKey)

class Column(SAColumn, SNVersionMixin, SNPatchMixin):
    inherit_cache = True
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable[Arguments]:
        column_type_object = column_type_map.new_from_xmlele(xmlele)
        arguments = Arguments(xmlele.getAttribute('name'), column_type_object)
        func0 = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
        func1 = lambda subxmlele: subxmlele.nodeName == 'ForeignKey'
        func2 = lambda subxmlele: ForeignKey.new_from_xmlele(subxmlele)
        arguments.append(*map(func2, filter(func1, filter(func0, xmlele.childNodes))))
        arguments.update_by_xmlattrs(
            xmlele, primary_key = bool_parser, unique = bool_parser, index = bool_parser,
            nullable = bool_parser, default = column_type_object.parse)
        yield arguments(cls)
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[ColumnTypeMixin], **kwargs) -> None:
        assert(len(objgroup) == 1)
        column = objgroup[0]
        attrs = [('name', column.name)]
        column.type = SA2SN.cast(column.type)
        attrs.extend(list(column.type.save_to_attrs()))
        if column.primary_key: attrs.append(('primary_key', 'True'))
        elif column.unique: attrs.append(('unique', 'True'))
        elif not column.nullable: attrs.append(('nullable', 'False'))
        if not isinstance(column.default, SAColumnDefault): assert(column.default is None)
        else:
            assert(column.default.is_scalar) # support scalar now.
            attrs.append(('default', column.type.format(column.default.arg)))
        for attrname, attrvalue in attrs: xmlele.setAttribute(attrname, attrvalue)
        column.type.save_to_subeles(xmlele)
        tuple(map(lambda foreign_key: foreign_key.save_to_subeles(xmlele),
                  map(lambda foreign_key: SA2SN.cast(foreign_key),
                      sorted(column.foreign_keys,
                             key = lambda foreign_key: foreign_key.target_fullname))))

    @classmethod
    def patch_forward_create(cls, xmlpatch: Element, table: SATable,
                             database_name: str = None) -> Iterable:
        column = table.columns[xmlpatch.getAttribute('name')]
        yield CreateColumn(database_name, column)
    @classmethod
    def patch_forward(cls, xmlpatch: Element, table0: SATable, table1: SATable,
                      database_name = None) -> Iterable:
        column0 = table0.columns[xmlpatch.getAttribute('name')]
        column1 = table1.columns[xmlpatch.getAttribute('name')]
        alter_column_operation = AlterColumn(database_name, column0, column1)
        if alter_column_operation.check_arguments(): yield alter_column_operation
    @classmethod
    def patch_forward_rename(cls, xmlpatch: Element, table0: SATable, table1: SATable,
                             database_name: str = None) -> Iterable:
        column0 = table0.columns[xmlpatch.getAttribute('name0')]
        column1 = table1.columns[xmlpatch.getAttribute('name1')]
        alter_column_operation = AlterColumn(database_name, column0, column1)
        if alter_column_operation.check_arguments(): yield alter_column_operation
    @classmethod
    def patch_forward_drop(cls, xmlpatch: Element, table: SATable,
                           database_name: str = None) -> Iterable:
        column = table.columns[xmlpatch.getAttribute('name')]
        yield DropColumn(database_name, column)
    @classmethod
    def patch_backward_create(cls, xmlpatch: Element, table: SATable,
                              database_name: str = None) -> Iterable:
        column = table.columns[xmlpatch.getAttribute('name')]
        yield DropColumn(database_name, column)
    @classmethod
    def patch_backward(cls, xmlpatch: Element, table1: SATable, table0: SATable,
                       database_name: str = None) -> Iterable:
        column1 = table1.columns[xmlpatch.getAttribute('name')]
        column0 = table0.columns[xmlpatch.getAttribute('name')]
        alter_column_operation = AlterColumn(database_name, column1, column0)
        if alter_column_operation.check_arguments(): yield alter_column_operation
    @classmethod
    def patch_backward_rename(cls, xmlpatch: Element, table1: SATable, table0: SATable,
                              database_name: str = None) -> Iterable:
        column1 = table1.columns[xmlpatch.getAttribute('name1')]
        column0 = table0.columns[xmlpatch.getAttribute('name0')]
        alter_column_operation = AlterColumn(database_name, column1, column0)
        if alter_column_operation.check_arguments(): yield alter_column_operation
    @classmethod
    def patch_backward_drop(cls, xmlpatch: Element, table: SATable,
                            database_name: str = None) -> Iterable:
        column = table.columns[xmlpatch.getAttribute('name')]
        yield CreateColumn(database_name, column)
SA2SN.register(Column)

class PrimaryKeyConstraint(SAPrimaryKeyConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        func0 = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
        func1 = lambda subxmlele: subxmlele.nodeName == 'Column'
        func2 = lambda subxmlele: subxmlele.getAttribute('name')
        arguments = Arguments(
            *map(func2, filter(func1, filter(func0, xmlele.childNodes))),
            name = xmlele.getAttribute('name'))
        yield arguments(cls)
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> None:
        assert(len(objgroup) == 1)
        primary_key = objgroup[0]
        xmlele.setAttribute('name', primary_key.name)
        for column in primary_key.columns:
            column_xmlele = xmlele.ownerDocument.createElement('Column')
            column_xmlele.setAttribute('name', column.name)
            xmlele.appendChild(column_xmlele)
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(PrimaryKeyConstraint)

class ForeignKeyConstraint(SAForeignKeyConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> None:
        pass # fixme:...
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(ForeignKeyConstraint)

class UniqueConstraint(SAUniqueConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> None:
        assert(len(objgroup) == 1)
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(UniqueConstraint)

class CheckConstraint(SACheckConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> None:
        assert(len(objgroup) == 1)
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(CheckConstraint)

class Index(SAIndex, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> None:
        assert(len(objgroup) == 1)
        pass # fixme.

    @classmethod
    def patch_forward_create(cls, xmlpatch: Element, table: SATable,
                             database_name: str = None) -> Iterable:
        index = table.indexes[xmlpatch.getAttribute('name')]
        yield CreateIndex(database_name, index)
    @classmethod
    def patch_forward(cls, xmlpatch: Element, table0: SATable, table1: SATable,
                      database_name: str = None) -> Iterable:
        index0 = table0.indexes[xmlpatch.getAttribute('name')]
        index1 = table1.indexes[xmlpatch.getAttribute('name')]
        yield DropIndex(database_name, index0)
        yield CreateIndex(database_name, index1)
    @classmethod
    def patch_forward_rename(cls, xmlpatch: Element, table0: SATable, table1: SATable,
                             database_name: str = None) -> Iterable:
        index0 = table0.indexes[xmlpatch.getAttribute('name0')]
        index1 = table1.indexes[xmlpatch.getAttribute('name1')]
        yield DropIndex(database_name, index0)
        yield CreateIndex(database_name, index1)
    @classmethod
    def patch_forward_drop(cls, xmlpatch: Element, table: SATable,
                           database_name: str = None) -> Iterable:
        index = table.indexes[xmlpatch.getAttribute('name')]
        yield DropIndex(database_name, index)
    @classmethod
    def patch_backward_create(cls, xmlpatch: Element, table: SATable,
                              database_name: str = None) -> Iterable:
        index = table.indexes[xmlpatch.getAttribute('name')]
        yield DropIndex(database_name, index)
    @classmethod
    def patch_backward(cls, xmlpatch: Element, table1: SATable, table0: SATable,
                       database_name: str = None) -> Iterable:
        index1 = table1.indexes[xmlpatch.getAttribute('name')]
        index0 = table0.indexes[xmlpatch.getAttribute('name')]
        yield DropIndex(database_name, index1)
        yield CreateIndex(database_name, index0)
    @classmethod
    def patch_backward_rename(cls, xmlpatch: Element, table1: SATable, table0: SATable,
                              database_name: str = None) -> Iterable:
        index1 = table1.indexes[xmlpatch.getAttribute('name1')]
        index0 = table0.indexes[xmlpatch.getAttribute('name0')]
        yield DropIndex(database_name, index1)
        yield CreateIndex(database_name, index0)
    @classmethod
    def patch_backward_drop(cls, xmlpatch: Element, table: SATable,
                            database_name: str = None) -> Iterable:
        index = table.indexes[xmlpatch.getAttribute('name')]
        yield CreateIndex(database_name, index)
SA2SN.register(Index)

class BaseTable(SATable, SNVersionMixin, SNPatchMixin):
    __sub_version_types__ = dict()
    __sub_patch_create_types__, __sub_patch_types__ = dict(), dict()
    __sub_patch_rename_types__, __sub_patch_drop_types__ = dict(), dict()
    __constraint_priority__ = {
        'PrimaryKeyConstraint': 1, 'ForeignKeyConstraint': 2,
        'UniqueConstraint': 3, 'CheckConstraint': 4 }
    def __new__(cls, *args, database_names: set[str] = None,
                component = None, table_priority = None, **kwargs):
        table = super().__new__(cls, *args, **kwargs)
        table.__database_names__ = database_names
        table.__component__ = component
        table.__table_priority__ = table_priority
        return table
    @property
    def key(self) -> str:
        if getattr(self, '__table_priority__', None) is None: return super().key
        if isinstance(self.__table_priority__, str): return self.__table_priority__
        elif isinstance(self.__table_priority__, int):
            return 'int.%06u' % self.__table_priority__
        else: raise ValueError('Unsupported type of __table_priority__(%r) for %r.' %
                               (self.__table_priority__, self))

    def __repr__(self):
        if getattr(self, '__database_names__', None) is None: str_database_names = 'None'
        else: str_database_names = '/'.join(sorted(self.__database_names__))
        return '%s(%s@%s)' % (self.__class__.__name__, self.name, str_database_names)
    def show(self): return '%s(%s)' % (repr(self), ', '.join(
            [repr(self.metadata), *map(lambda column: repr(column), self.columns),
             *map(lambda k: '%s=%s' % (k, repr(getattr(self, k))), ['schema'])]))
    def generate_subobjs(self, **kwargs) -> tuple[object]:
        func0 = lambda constraint: len(constraint.columns) > 1
        func1 = lambda constraint: self.__constraint_priority__[constraint.__class__.__name__]
        return (*self.columns,
                *sorted(filter(func0, self.constraints), key = func1),
                *self.indexes)
BaseTable.register_subtypes(Column, PrimaryKeyConstraint, ForeignKeyConstraint,
                            UniqueConstraint, CheckConstraint, Index)

class Table(BaseTable):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        table_name = xmlele.getAttribute('name')
        database_names = metadata.params.get_one(table_name, Context()).get_one(
            'database_names', { metadata.settings.default_database_name })
        yield cls(table_name, metadata, database_names = database_names,
                  *cls.load_subobjs_from_xmlele(xmlele, metadata))
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin | ColumnTypeMixin],
                            **kwargs) -> None:
        assert(len(objgroup) == 1)
        xmlele.setAttribute('name', objgroup[0].name)
    @classmethod
    def save_params(cls, params: Context,
                    objgroup: tuple[SNBaseMixin | ColumnTypeMixin]) -> Context:
        assert(len(objgroup) == 1)
        self_params = Context(database_names = objgroup[0].__database_names__)
        if bool(self_params): params.update(**{ objgroup[0].name: self_params })
        return params

    @classmethod
    def params_update(cls, xmlversion: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        table0 = metadata0.tables[xmlversion.getAttribute('name')]
        table1 = metadata0.tables[xmlversion.getAttribute('name')]
        for database_name in sorted(table0.__database_names__ | table1.__database_names__):
            if database_name not in table0.__database_names__:
                yield CreateTable(database_name, table1)
            elif database_name not in table1.__database_names__:
                yield DropTable(database_name, table0)
            else:
                for operation in cls.do_params_update(
                        xmlversion, table0, table1, database_name = database_name):
                    yield operation
    @classmethod
    def patch_forward_create(cls, xmlpatch: Element,
                             metadata: SAMetaData) -> Iterable[BaseOperation]:
        table = metadata.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table.__database_names__):
            yield CreateTable(database_name, table)
    @classmethod
    def patch_forward(cls, xmlpatch: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        table0 = metadata0.tables[xmlpatch.getAttribute('name')]
        table1 = metadata1.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table0.__database_names__ | table1.__database_names__):
            if database_name not in table0.__database_names__:
                yield CreateTable(database_name, table1)
            elif database_name not in table1.__database_names__:
                yield DropTable(database_name, table0)
            else:
                for operation in cls.do_forward(
                        xmlpatch, table0, table1, database_name = database_name):
                    yield operation
    @classmethod
    def patch_forward_rename(cls, xmlpatch: Element,
                             metadata0: SAMetaData,
                             metadata1: SAMetaData) -> Iterable[BaseOperation]:
        table0 = metadata0.tables[xmlpatch.getAttribute('name0')]
        table1 = metadata1.tables[xmlpatch.getAttribute('name1')]
        for database_name in sorted(table0.__database_names__ | table1.__database_names__):
            if database_name not in table0.__database_names__:
                yield CreateTable(database_name, table1)
            elif database_name not in table1.__database_names__:
                yield DropTable(database_name, table0)
            else:
                yield RenameTable(database_name, table0, table1)
                for operation in cls.do_forward(
                        xmlpatch, table0, table1, database_name = database_name):
                    yield operation
    @classmethod
    def patch_forward_drop(cls, xmlpatch: Element,
                           metadata: SAMetaData) -> Iterable[BaseOperation]:
        table = metadata.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table.__database_names__, reverse = True):
            yield DropTable(database_name, table)
    @classmethod
    def patch_backward_create(cls, xmlpatch: Element,
                              metadata: SAMetaData) -> Iterable[BaseOperation]:
        table = metadata.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table.__database_names__, reverse = True):
            yield DropTable(database_name, table)
    @classmethod
    def patch_backward(cls, xmlpatch: Element,
                       metadata1: SAMetaData,
                       metadata0: SAMetaData) -> Iterable[BaseOperation]:
        table1 = metadata1.tables[xmlpatch.getAttribute('name')]
        table0 = metadata0.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table1.__database_names__ | table0.__database_names__):
            if database_name not in table1.__database_names__:
                yield CreateTable(database_name, table0)
            elif database_name not in table0.__database_names__:
                yield DropTable(database_name, table1)
            else:
                for operation in cls.do_backward(
                        xmlpatch, table1, table0, database_name = database_name):
                    yield operation
    @classmethod
    def patch_backward_rename(cls, xmlpatch: Element,
                              metadata1: SAMetaData,
                              metadata0: SAMetaData) -> Iterable[BaseOperation]:
        table1 = metadata1.tables[xmlpatch.getAttribute('name1')]
        table0 = metadata0.tables[xmlpatch.getAttribute('name0')]
        for database_name in sorted(table1.__database_names__ | table0.__database_names__):
            if database_name not in table1.__database_names__:
                yield CreateTable(database_name, table0)
            elif database_name not in table0.__database_names__:
                yield DropTable(database_name, table1)
            else:
                for patch_context in cls.do_backward(
                        xmlpatch, table1, table0, database_name = database_name):
                    yield patch_context
                yield RenameTable(database_name, table1, table0)
    @classmethod
    def patch_backward_drop(cls, xmlpatch: Element,
                            metadata: SAMetaData) -> Iterable[BaseOperation]:
        table = metadata.tables[xmlpatch.getAttribute('name')]
        for database_name in sorted(table.__database_names__):
            yield CreateTable(database_name, table)

class ShardTable(BaseTable):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        shard_name = xmlele.getAttribute('name')
        dbname2suffixes = metadata.params.get_one(
            shard_name, Context()).get_one('database_names', {})
        for dbname, suffixes in dbname2suffixes.items():
            for shard_suffix in suffixes:
                yield cls('%s_%s' % (shard_name, shard_suffix),
                          metadata, database_names = { dbname },
                          shard_name = shard_name, shard_suffix = shard_suffix,
                          *cls.load_subobjs_from_xmlele(xmlele, metadata))
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin | ColumnTypeMixin],
                            **kwargs) -> None:
        xmlele.setAttribute('name', objgroup[0].name)
    @classmethod
    def save_params(cls, params: Context, objgroup: tuple[SNBaseMixin]) -> Context:
        self_params = Context(database_names = dict())
        func0 = lambda obj: obj.__database_names__
        func1 = lambda database_names: database_names is not None
        for dbname in set().union(*filter(func1, map(func0, objgroup))):
            func0 = lambda obj: dbname in obj.__database_names__
            func1 = lambda obj: obj.__shard_suffix__
            suffixes = sorted(map(func1, filter(func0, objgroup)))
            self_params.database_names[dbname] = suffixes
        if not self_params.database_names: return params
        return params.set_one(objgroup[0].__shard_name__, self_params)

    def __new__(cls, *args, shard_name: str | None = None,
                shard_suffix: str | None = None, **kwargs):
        table = super().__new__(cls, *args, **kwargs)
        table.__shard_name__, table.__shard_suffix__ = shard_name, shard_suffix
        return table
    def group_check(self, other) -> bool:
        return self.__shard_name__ == other.__shard_name__

    @classmethod
    def params_update(cls, xmlversion: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlversion.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: (table.__shard_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for shard_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(shard_suffix, None), tables1.get(shard_suffix, None)
            if table0 is not None:
                assert(len(table0.__database_names__) == 1)
                database_name0 = tuple(table0.__database_names__)[0]
            if table1 is not None:
                assert(len(table1.__database_names__) == 1)
                database_name1 = tuple(table1.__database_names__)[0]
            if table0 is None: yield CreateTable(database_name1, table1)
            elif table1 is None: yield DropTable(database_name0, table0)
            elif database_name0 == database_name1:
                for operation in cls.do_params_update(
                        xmlversion, table0, table1, database_name = database_name0):
                    yield operation
            else:
                yield CreateTable(database_name1, table1)
                yield DropTable(database_name0, table0)
    @classmethod
    def patch_forward_create(cls, xmlpatch: Element,
                             metadata: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: table.__shard_name__ == name
        func1 = lambda table: table.__shard_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1):
            for database_name in sorted(table.__database_names__):
                yield CreateTable(database_name, table)
    @classmethod
    def patch_forward(cls, xmlpatch: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: (table.__shard_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for shard_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(shard_suffix, None), tables1.get(shard_suffix, None)
            if table0 is not None:
                assert(len(table0.__database_names__) == 1)
                database_name0 = tuple(table0.__database_names__)[0]
            if table1 is not None:
                assert(len(table1.__database_names__) == 1)
                database_name1 = tuple(table1.__database_names__)[0]
            if table0 is None: yield CreateTable(database_name1, table1)
            elif table1 is None: yield DropTable(database_name0, table0)
            elif database_name0 == database_name1:
                for operation in cls.do_forward(
                        xmlpatch, table0, table1, database_name = database_name0):
                    yield operation
            else:
                yield CreateTable(database_name1, table1)
                yield DropTable(database_name0, table0)
    @classmethod
    def patch_forward_rename(cls, xmlpatch: Element,
                             metadata0: SAMetaData,
                             metadata1: SAMetaData) -> Iterable[BaseOperation]:
        name0, name1 = xmlpatch.getAttribute('name0'), xmlpatch.getAttribute('name1')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name0
        func1 = lambda table: (table.__shard_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        func0 = lambda table: table.__shard_name__ == name1
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for shard_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(shard_suffix, None), tables1.get(shard_suffix, None)
            if table0 is not None:
                assert(len(table0.__database_names__) == 1)
                database_name0 = tuple(table0.__database_names__)[0]
            if table1 is not None:
                assert(len(table1.__database_names__) == 1)
                database_name1 = tuple(table1.__database_names__)[0]
            if table0 is None: yield CreateTable(database_name1, table1)
            elif table1 is None: yield DropTable(database_name0, table0)
            elif database_name0 == database_name1:
                yield RenameTable(database_name0, table0, table1)
                for operation in cls.do_forward(
                        xmlpatch, table0, table1, database_name = database_name):
                    yield operation
            else:
                yield CreateTable(database_name1, table1)
                yield DropTable(database_name0, table0)
    @classmethod
    def patch_forward_drop(cls, xmlpatch: Element,
                           metadata: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: table.__shard_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1, reverse = True):
            for database_name in sorted(table.__database_names__, reverse = True):
                yield DropTable(database_name, table)
    @classmethod
    def patch_backward_create(cls, xmlpatch: Element,
                              metadata: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: table.__shard_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1, reverse = True):
            for database_name in sorted(table.__database_names__, reverse = True):
                yield DropTable(database_name, table)
    @classmethod
    def patch_backward(cls, xmlpatch: Element,
                       metadata1: SAMetaData,
                       metadata0: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: (table.__shard_suffix__, table)
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        for shard_suffix in sorted(set(tables1.keys()) | set(tables0.keys())):
            table1, table0 = tables1.get(shard_suffix, None), tables0.get(shard_suffix, None)
            if table1 is not None:
                assert(len(table1.__database_names__) == 1)
                database_name1 = tuple(table1.__database_names__)[0]
            if table0 is not None:
                assert(len(table0.__database_names__) == 1)
                database_name0 = tuple(table0.__database_names__)[0]
            if table1 is None: yield CreateTable(database_name0, table0)
            elif table0 is None: yield DropTable(database_name1, table1)
            elif database_name1 == database_name0:
                for operation in cls.do_backward(
                        xmlpatch, table1, table0, database_name = database_name0):
                    yield operation
            else:
                yield CreateTable(database_name0, table0)
                yield DropTable(database_name1, table1)
    @classmethod
    def patch_backward_rename(cls, xmlpatch: Element,
                              metadata1: SAMetaData,
                              metadata0: SAMetaData) -> Iterable[BaseOperation]:
        name1, name0 = xmlpatch.getAttribute('name1'), xmlpatch.getAttribute('name0')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name1
        func1 = lambda table: (table.__shard_suffix__, table)
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name0
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        for shard_suffix in sorted(set(tables1.keys()) | set(tables0.keys())):
            table1, table0 = tables1.get(shard_suffix, None), tables0.get(shard_suffix, None)
            if table1 is not None:
                assert(len(table1.__database_names__) == 1)
                database_name1 = tuple(table1.__database_names__)[0]
            if table0 is not None:
                assert(len(table0.__database_names__) == 1)
                database_name0 = tuple(table0.__database_names__)[0]
            if table1 is None: yield CreateTable(database_name0, table0)
            elif table0 is None: yield DropTable(database_name1, table1)
            elif database_name1 == database_name0:
                yield RenameTable(database_name0, table1, table0)
                for operation in cls.do_backward(
                        xmlpatch, table1, table0, database_name = database_name0):
                    yield operation
            else:
                yield CreateTable(database_name0, table0)
                yield DropTable(database_name1, table1)
    @classmethod
    def patch_backward_drop(cls, xmlpatch: Element,
                            metadata: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__shard_name__ == name
        func1 = lambda table: table.__shard_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1):
            for database_name in sorted(table.__database_names__):
                yield CreateTable(database_name, table)
