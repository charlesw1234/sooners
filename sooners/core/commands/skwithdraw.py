from ...db.migration import Context, DefaultDict, BaseOperation, BaseComponent, Migration
from .libs.schemabase import BaseSchemaCommand

class MigrationWithdraw(Migration):
    def do_operation_ornot(self, operation: BaseOperation,
                           dbname2key2record: DefaultDict) -> bool:
        pass

    def _generate_operations(self, component: BaseComponent) -> tuple[BaseOperation]:
        return tuple(reversed(super()._generate_operations(component)))

    def _do_operation(self, context: Context, operation: BaseOperation,
                      component: BaseComponent, dbname2key2record: DefaultDict) -> None:
        pass # fixme.

class Command(BaseSchemaCommand):
    help = ('Withdraw the broken database schema migration.')
    def sub_handle(self, context: Context, migration: MigrationWithdraw) -> None:
        member = migration.direction_revert_member(context.exctype)
        self.do_delayed_operations(context, member(context))
