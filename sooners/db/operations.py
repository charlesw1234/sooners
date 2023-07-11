from sqlalchemy import Table as SATable, Column as SAColumn, Index as SAIndex
from sqlalchemy import PrimaryKeyConstraint as SAPrimaryKeyConstraint
from sqlalchemy import ForeignKeyConstraint as SAForeignKeyConstraint
from sqlalchemy import UniqueConstraint as SAUniqueConstraint
from sqlalchemy import CheckConstraint as SACheckConstraint
from sqlalchemy import ColumnDefault as SAColumnDefault
from alembic.operations import Operations as AlembicOperations
from ..utils import Arguments

class OperationMeta(type):
    typeid2class = dict()
    def __new__(metaclass, classname: str,
                bases: tuple[type], classdict: dict[str, object], **kwargs):
        cls = super().__new__(metaclass, classname, bases, classdict, **kwargs)
        if hasattr(cls, 'typeid'): metaclass.typeid2class[cls.typeid] = cls
        return cls

class BaseOperation(object, metaclass = OperationMeta):
    def __init__(self, database_name: str, *args) -> None:
        self.database_name = database_name
        for index, attr in enumerate(self.attrs): setattr(self, attr, args[index])
    def __repr__(self) -> str:
        names_str = ', '.join(map(lambda attr: getattr(self, attr).name, self.attrs))
        return '%s@%s(%s)' % (self.__class__.__name__, self.database_name, names_str)
    def __call__(self, prompt: callable, oper: AlembicOperations) -> object:
        arguments = self.make_arguments(oper)
        prompt('%s@%s(%r)' % (self.oper_member, self.database_name, arguments))
        result = arguments(getattr(oper, self.oper_member))
        if callable(getattr(self, 'post_operation', None)): self.post_operation(prompt, oper)
        oper.get_bind().commit()
        return result
    def table_name(self) -> str | None: return None
    def names(self) -> tuple[str | None]:
        return (None if len(self.attrs) < 1 else getattr(self, self.attrs[0]).name,
                None if len(self.attrs) < 2 else getattr(self, self.attrs[1]).name)
    def key(self) -> tuple[str | int | None]:
        return (self.typeid, self.table_name(), *self.names())


ColumnDict = dict[str, SAColumn]
class CreateTable(BaseOperation):
    typeid, oper_member, attrs = 1, 'create_table', ('table',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        from .table import PrimaryKeyConstraint, ForeignKeyConstraint
        from .table import UniqueConstraint, CheckConstraint, Index
        columns = tuple(map(self._clone_column, self.table.columns))
        column_dict = dict(map(lambda column: (column.name, column), columns))
        func0 = lambda constraint: isinstance(constraint, PrimaryKeyConstraint)
        func1 = lambda constraint: self._clone_primary_key_constraint(constraint, column_dict)
        primary_key_constraints = map(func1, filter(func0, self.table.constraints))
        func0 = lambda constraint: isinstance(constraint, ForeignKeyConstraint)
        func1 = lambda constraint: self._clone_foreign_key_constraint(constraint, column_dict)
        foreign_key_constraints = map(func1, filter(func0, self.table.constraints))
        func0 = lambda constraint: isinstance(constraint, UniqueConstraint)
        func1 = lambda constraint: self._clone_unique_constraint(constraint, column_dict)
        unique_constraints = map(func1, filter(func0, self.table.constraints))
        func0 = lambda constraint: isinstance(constraint, CheckConstraint)
        func1 = lambda constraint: self._clone_check_constraint(constraint, column_dict)
        check_constraints = map(func1, filter(func0, self.table.constraints))
        func0 = lambda index: isinstance(index, Index)
        func1 = lambda index: self._clone_index(index, column_dict)
        indexes = map(func1, filter(func0, self.table.indexes))
        return Arguments(self.table.name, *columns,
                         *primary_key_constraints, *foreign_key_constraints,
                         *unique_constraints, *check_constraints, *indexes)
    def _clone_column(self, column: SAColumn) -> SAColumn:
        from .table import Column
        arguments = Arguments(column.name, column.type)
        fields = ('key', 'primary_key', 'nullable', 'default', 'server_default',
                  'server_onupdate', 'index', 'unique', 'system', 'doc', 'onupdate',
                  'autoincrement', 'comment')
        arguments.update(**dict(map(lambda field: (field, getattr(column, field)), fields)))
        return arguments(Column)
    def _clone_primary_key_constraint(self, constraint: SAPrimaryKeyConstraint,
                                      column_dict: ColumnDict) -> SAPrimaryKeyConstraint:
        from .table import PrimaryKeyConstraint
        columns = map(lambda column: column_dict[column.name], constraint.columns)
        return Arguments(*columns, name = constraint.name)(PrimaryKeyConstraint)
    def _clone_foreign_key_constraint(self, constraint: SAForeignKeyConstraint,
                                      column_dict: ColumnDict) -> SAForeignKeyConstraint:
        from .table import ForeignKeyConstraint
        columns, refcolumns = list(), list()
        for column_key, foreign_key in constraint._elements.items():
            columns.append(column_key)
            refcolumns.append(foreign_key._get_colspec())
        return Arguments(columns, refcolumns, name = constraint.name,
                         onupdate = constraint.onupdate, ondelete = constraint.ondelete,
                         use_alter = constraint.use_alter)(ForeignKeyConstraint)
    def _clone_unique_constraint(self, constraint: SAUniqueConstraint,
                                 column_dict: ColumnDict) -> SAUniqueConstraint:
        from .table import UniqueConstraint
        columns = map(lambda column: column_dict[column.name], constraint.columns)
        return Arguments(*columns, name = constraint.name)(UniqueConstraint)
    def _clone_check_constraint(self, constraint: SACheckConstraint,
                                column_dict: ColumnDict) -> SACheckConstraint:
        from .table import CheckConstraint
        return Arguments(constraint.sqltext.text)(CheckConstraint)
    def _clone_index(self, index: SAIndex, column_dict: ColumnDict) -> SAIndex:
        from .table import Index
        func = lambda column: column_dict[column.name]
        return Arguments(index.name, map(func, index.columns), unique = index.unique)(Index)

class RenameTable(BaseOperation):
    typeid, oper_member, attrs = 2, 'rename_table', ('table0', 'table1')
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.table0.name, self.table1.name)

class DropTable(BaseOperation):
    typeid, oper_member, attrs = 3, 'drop_table', ('table',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.table.name)
    def post_operation(self, prompt: callable, oper: AlembicOperations) -> None:
        func = lambda column: callable(getattr(column.type, 'post_operation', None))
        for column in filter(func, self.table.columns):
            column.type.post_operation(prompt, self.database_name, oper)

class ColumnOperationMixin(object):
    def table_name(self) -> str | None: return self.column.table.name

class CreateColumn(ColumnOperationMixin, BaseOperation):
    typeid, oper_member, attrs = 4, 'add_column', ('column',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.column.table.name, self.column)

class AlterColumn(ColumnOperationMixin, BaseOperation):
    typeid, oper_member, attrs = 5, 'alter_column', ('column0', 'column1')
    def check_arguments(self) -> bool: return bool(self._make_arguments0())
    def make_arguments(self, oper: Arguments) -> Arguments:
        return self._make_arguments1(self._make_arguments0(), oper)
    def _make_arguments0(self) -> Arguments:
        arguments = Arguments()
        if self.column0.name != self.column1.name:
            arguments.update(new_column_name = self.column1.name)
        if not self.column0.type.equal(self.column1.type):
            arguments.update(type_ = self.column1.type)
        if self.column0.nullable != self.column1.nullable:
            arguments.update(nullable = self.column1.nullable)
        func = lambda default: default.arg if isinstance(default, SAColumnDefault) else default
        default0, default1 = func(self.column0.default), func(self.column1.default)
        if default0 != default1: arguments.update(server_default = default1)
        if self.column0.comment != self.column1.comment:
            arguments.update(comment = self.column1.comment)
        return arguments
    def _make_arguments1(self, arguments: Arguments, oper: AlembicOperations) -> Arguments:
        from alembic.ddl.sqlite import SQLiteImpl
        from alembic.ddl.mysql import MySQLImpl
        from alembic.ddl.postgresql import PostgresqlImpl
        if isinstance(oper.impl, SQLiteImpl): pass
        elif isinstance(oper.impl, MySQLImpl):
            arguments.update(
                type_ = self.column1.type, nullable = self.column1.nullable,
                existing_type = self.column0.type,
                existing_server_default = self.column0.default,
                existing_nullable = self.column0.nullable)
        elif isinstance(oper.impl, PostgresqlImpl): pass
        return arguments.prepend(self.column1.table.name, self.column0.name)

class DropColumn(ColumnOperationMixin, BaseOperation):
    typeid, oper_member, attrs = 6, 'drop_column', ('column',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.column.table.name, self.column.name)
    def post_operation(self, prompt: callable, oper: AlembicOperations) -> None:
        if callable(getattr(self.column.type, 'post_operation', None)):
            self.column.type.post_operation(prompt, self.database_name, oper)

class ConstraintOperationMixin(object):
    def table_name(self) -> str | None: return self.constraint.table.name

class CreatePrimaryKeyConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 7, 'create_primary_key_constraint'
    # fixme.
class DropPrimaryKeyConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 8, 'drop_primary_key_constraint'
    # fixme

class CreateForeignKeyConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 9, 'create_foreign_key_constraint'
    # fixme.
class DropForeignKeyConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 10, 'drop_foreign_key_constraint'
    # fixme.

class CreateUniqueConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 11, 'create_unique_constraint'
    # fixme.
class DropUniqueConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 12, 'drop_unique_constraint'
    # fixme.

class CreateCheckConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 13, 'create_check_constraint'
    # fixme.
class DropCheckConstraint(ConstraintOperationMixin, BaseOperation):
    typeid, oper_member = 14, 'drop_check_constraint'
    # fixme.

class IndexOperationMixin(object):
    def table_name(self) -> str | None: return self.index.table.name

class CreateIndex(IndexOperationMixin, BaseOperation):
    typeid, oper_member, attrs = 15, 'create_index', ('index',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.index.name, self.index.table.name,
                         self.index.columns, unique = self.index.unique)

class DropIndex(IndexOperationMixin, BaseOperation):
    typeid, oper_member, attrs = 16, 'drop_index', ('index',)
    def make_arguments(self, oper: AlembicOperations) -> Arguments:
        return Arguments(self.index.name, self.index.table.name)
