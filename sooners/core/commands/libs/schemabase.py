from argparse import ArgumentParser, Namespace
from ....command import BaseCommand
from ....utils import Context

class BaseSchemaCommand(BaseCommand):
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('--no-action', action = 'store_true')
        parser.add_argument('--debug-schema', action = 'store_true')

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        context = self.settings.make_migrate_context(
            do_action = not namespace.no_action,
            debug_schema = namespace.debug_schema,
            exctype = self.exctype, prompt = self.prompt)
        migration = self.migration_class(context)
        if migration.is_clean(): raise RuntimeError('The migration is clean now.')
        self.sub_handle(context, migration)

    def do_delayed_operations(self, context: Context, delayed_operations) -> None:
        for delayed_operation in delayed_operations:
            delayed_operation(context.prompt,
                              context.operators[delayed_operation.database_name])
