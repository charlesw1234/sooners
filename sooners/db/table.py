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

    def save_to_subeles(self, xmlele: Element) -> str:
        xmlele.appendChild(subxmlele := xmlele.ownerDocument.createElement('ForeignKey'))
        attrs = [('column', self.target_fullname)]
        if self.name is not None: attrs.append(('name', self.name))
        if self.onupdate is not None: attrs.append(('onupdate', self.onupdate))
        if self.ondelete is not None: attrs.append(('ondelete', self.ondelete))
        tuple(map(lambda attr: subxmlele.setAttribute(attr[0], attr[1]), attrs))
        func = lambda attr: '%s="%s"' % attr
        return '<ForeignKey %s/>' % ' '.join(map(func, attrs))
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
                            objgroup: tuple[ColumnTypeMixin], **kwargs) -> str:
        assert(len(objgroup) == 1)
        column = objgroup[0]
        attrs = [('name', column.name)]
        attrs.extend(list(column.type.save_to_attrs()))
        if column.primary_key: attrs.append(('primary_key', 'True'))
        elif column.unique: attrs.append(('unique', 'True'))
        elif not column.nullable: attrs.append(('nullable', 'False'))
        if not isinstance(column.default, SAColumnDefault): assert(column.default is None)
        else:
            assert(column.default.is_scalar) # support scalar now.
            attrs.append(('default', column.type.format(column.default.arg)))
        for attrname, attrvalue in attrs: xmlele.setAttribute(attrname, attrvalue)
        func = lambda attr: '%s="%s"' % attr
        attrs_str = ' '.join(map(func, attrs))
        subeles_str0 = column.type.save_to_subeles(xmlele)
        subeles_str1 = ''.join(
            map(lambda foreign_key: foreign_key.save_to_subeles(xmlele),
                map(lambda foreign_key: SA2SN.cast(foreign_key),
                    sorted(column.foreign_keys,
                           key = lambda foreign_key: foreign_key.target_fullname))))
        subeles_str = subeles_str0 + subeles_str1
        if not subeles_str: return '<%s name="%s"/>' % (cls.__name__, attrs_str)
        else: return '<%s name="%s">%s' % (cls.__name__, attrs_str, subeles_str)
    @classmethod
    def save_to_xmlele_close(cls, xmlele: Element,
                             objgroup: tuple[ColumnTypeMixin], **kwargs) -> str:
        column = objgroup[0]
        if not column.foreign_keys: return ''
        else: return '<%s>' % (cls.__name__,)

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
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        assert(len(objgroup) == 1)
        return '<%s/>' % cls.__name__ # fixme.
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(PrimaryKeyConstraint)

class ForeignKeyConstraint(SAForeignKeyConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        # fixme:...
        return '<%s/>' % cls.__name__ # fixme.
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(ForeignKeyConstraint)

class UniqueConstraint(SAUniqueConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        assert(len(objgroup) == 1)
        return '<%s/>' % cls.__name__ # fixme.
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(UniqueConstraint)

class CheckConstraint(SACheckConstraint, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        assert(len(objgroup) == 1)
        return '<%s/>' % cls.__name__ # fixme.
    # add patch_forward_create/patch_forward/patch_forward_rename/patch_forward_drop here.
    # add patch_backward_create/patch_backward/patch_backward_rename/patch_backward_drop here.
SA2SN.register(CheckConstraint)

class Index(SAIndex, SNVersionMixin, SNPatchMixin):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        pass # fixme.
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        assert(len(objgroup) == 1)
        return '<%s/>' % cls.__name__ # fixme.

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
    @classmethod
    def save_to_xmlele_close(cls, xmlele: Element,
                             objgroup: tuple[SNBaseMixin | ColumnTypeMixin],
                             **kwargs) -> str:
        return '</%s>' % (cls.__name__,)
    def __new__(cls, *args, database_names: set[str] = None, component = None, **kwargs):
        table = super().__new__(cls, *args, **kwargs)
        table.__database_names__ = database_names
        table.__component__ = component
        return table
    def __repr__(self):
        if self.__database_names__ is None: str_database_names = 'None'
        else: str_database_names = '/'.join(sorted(self.__database_names__))
        return '%s(%s@%s)' % (self.__class__.__name__, self.name, str_database_names)
    def show(self): return '%s(%s)' % (repr(self), ', '.join(
            [repr(self.metadata), *map(lambda column: repr(column), self.columns),
             *map(lambda k: '%s=%s' % (k, repr(getattr(self, k))), ['schema'])]))
    def generate_subobjs(self, **kwargs) -> tuple[object]:
        return (*self.columns, *self.constraints, *self.indexes)
BaseTable.register_subtypes(Column, PrimaryKeyConstraint, ForeignKeyConstraint,
                            UniqueConstraint, CheckConstraint, Index)

class Table(BaseTable):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        table_name = xmlele.getAttribute('name')
        database_names = metadata.params.get(table_name, {}).get(
            'database_names', metadata.settings.default_database_names)
        yield cls(table_name, metadata, database_names = database_names,
                  *cls.load_subobjs_from_xmlele(xmlele, metadata))
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin | ColumnTypeMixin],
                            **kwargs) -> str:
        assert(len(objgroup) == 1)
        xmlele.setAttribute('name', objgroup[0].name)
        return '<%s name="%s">' % (cls.__name__, objgroup[0].name)
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
                        xmlpatch, table1, table0, datbase_name = database_name):
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

class BatchTable(BaseTable):
    @classmethod
    def new_from_xmlele(cls, xmlele: Element, metadata: SAMetaData) -> Iterable:
        batch_name = xmlele.getAttribute('name')
        for dbname, suffixes in metadata.params[batch_name]['database_names'].items():
            for batch_suffix in suffixes:
                yield cls('%s_%s' % (batch_name, batch_suffix),
                          metadata, database_names = { dbname },
                          batch_name = batch_name, batch_suffix = batch_suffix,
                          *cls.load_subobjs_from_xmlele(xmlele, metadata))
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin | ColumnTypeMixin],
                            **kwargs) -> str:
        xmlele.setAttribute('name', objgroup[0].name)
        return '<%s name="%s"/>' % (cls.__name__, objgroup[0].name)
    @classmethod
    def save_params(cls, params: Context, objgroup: tuple[SNBaseMixin]) -> Context:
        self_params = Context(database_names = dict())
        for dbname in set().union(*map(lambda obj: obj.__database_names__, objgroup)):
            func0 = lambda obj: dbname in obj.__database_names__
            func1 = lambda obj: obj.__batch_suffix__
            suffixes = sorted(map(func1, filter(func0, objgroup)))
            self_params.database_names[dbname] = suffixes
        params.update(**{ objgroup[0].__batch_name__: self_params })
        return params

    def __new__(cls, *args, batch_name: str | None = None,
                batch_suffix: str | None = None, **kwargs):
        table = super().__new__(cls, *args, **kwargs)
        table.__batch_name__, table.__batch_suffix__ = batch_name, batch_suffix
        return table
    def group_check(self, other) -> bool:
        return self.__batch_name__ == other.__batch_name__

    @classmethod
    def params_update(cls, xmlversion: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlversion.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: (table.__batch_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for batch_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(batch_suffix, None), tables1.get(batch_suffix, None)
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
        func0 = lambda table: table.__batch_name__ == name
        func1 = lambda table: table.__batch_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1):
            for database_name in sorted(table.__database_names__):
                yield CreateTable(database_name, table)
    @classmethod
    def patch_forward(cls, xmlpatch: Element,
                      metadata0: SAMetaData,
                      metadata1: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: (table.__batch_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for batch_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(batch_suffix, None), tables1.get(batch_suffix, None)
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
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name0
        func1 = lambda table: (table.__batch_suffix__, table)
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        func0 = lambda table: table.__batch_name__ == name1
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        for batch_suffix in sorted(set(tables0.keys()) | set(tables1.keys())):
            table0, table1 = tables0.get(batch_suffix, None), tables1.get(batch_suffix, None)
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
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: table.__batch_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1, reverse = True):
            for database_name in sorted(table.__database_names__, reverse = True):
                yield DropTable(database_name, table)
    @classmethod
    def patch_backward_create(cls, xmlpatch: Element,
                              metadata: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: table.__batch_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1, reverse = True):
            for database_name in sorted(table.__database_names__, reverse = True):
                yield DropTable(database_name, table)
    @classmethod
    def patch_backward(cls, xmlpatch: Element,
                       metadata1: SAMetaData,
                       metadata0: SAMetaData) -> Iterable[BaseOperation]:
        name = xmlpatch.getAttribute('name')
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: (table.__batch_suffix__, table)
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        for batch_suffix in sorted(set(tables1.keys()) | set(tables0.keys())):
            table1, table0 = tables1.get(batch_suffix, None), tables0.get(batch_suffix, None)
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
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name1
        func1 = lambda table: (table.__batch_suffix__, table)
        tables1 = dict(map(func1, filter(func0, metadata1.tables.values())))
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name0
        tables0 = dict(map(func1, filter(func0, metadata0.tables.values())))
        for batch_suffix in sorted(set(tables1.keys()) | set(tables0.keys())):
            table1, table0 = tables1.get(batch_suffix, None), tables0.get(batch_suffix, None)
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
        func0 = lambda table: isinstance(table, cls) and table.__batch_name__ == name
        func1 = lambda table: table.__batch_suffix__
        for table in sorted(filter(func0, metadata.tables), key = func1):
            for database_name in sorted(table.__database_names__):
                yield CreateTable(database_name, table)
