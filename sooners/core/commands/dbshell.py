from argparse import ArgumentParser, Namespace
from subprocess import run
from ...command import BaseCommand

class Command(BaseCommand):
    help = ('run command line shell for the specified database.')
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            'database', choices = self.settings.databases.keys(),
            help = 'Specify the database to be connected.')

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        dboper = self.settings.databases[namespace.database]
        run(dboper.dbshell(), env = dboper.dbshellenv())
