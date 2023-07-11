from ...db.migration import Context, DefaultDict, BaseOperation, Migration
from .libs.schemabase import BaseSchemaCommand

class MigrationContinue(Migration):
    def do_operation_ornot(self, operation: BaseOperation,
                           dbname2key2record: DefaultDict) -> bool:
        return operation.key() not in dbname2key2record[operation.database_name]

class Command(BaseSchemaCommand):
    help = ('Finish the broken database schema migration.')
    migration_class = MigrationContinue
    def sub_handle(self, context: Context, migration: MigrationContinue) -> None:
        member = migration.direction_member(context.exctype)
        self.do_delayed_operations(context, member(context))
