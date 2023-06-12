from argparse import ArgumentParser, Namespace
from importlib import import_module
from ...command import BaseCommand, locate_command
from ...component import BaseComponent

class Command(BaseCommand):
    help = ('show this help messages.')
    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument('command', nargs = '?', help = '')

    def handle(self, namespace: Namespace) -> None:
        if namespace.command is not None:
            command = locate_command(self.settings, namespace.command)
            command.make_parser().print_help()
        else:
            commands = list()
            for component in self.settings.components.values():
                func = lambda ctx: (ctx.component.name, ctx.name, ctx.klass.help)
                commands.extend(map(func, BaseCommand.scan_component(component)))
            maxlen0 = max(map(lambda command: len(command[0]), commands))
            maxlen1 = max(map(lambda command: len(command[1]), commands))
            fmtstr = ':'.join(('%%%us' % maxlen0, '%%%us' % maxlen1, '  %s'))
            for command in commands: print(fmtstr % command)
