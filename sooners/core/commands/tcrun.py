from argparse import ArgumentParser, Namespace
from fnmatch import fnmatch
from ...utils import Context
from ...component import BaseComponent
from ...command import BaseCommand
from ...testcase import BaseTestCase

class Command(BaseCommand):
    help = ('')
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('--dry-run', action = 'store_true')
        parser.add_argument(
            'pattern', nargs = '*', type = str,
            help = 'the pattern to match the name of testcase.')

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        namespace.results = Context(succ = 0, failed = 0)
        for component in self.settings.components.values():
            if not namespace.pattern: func = lambda testcase: True
            else: func = lambda testcase: any(map(
                    lambda pattern: fnmatch(
                        '%s.%s' % (component.name, testcase.name), pattern),
                    namespace.pattern))
            for testcase in filter(func, BaseTestCase.scan_component(component)):
                self.one_testcase(testcase, namespace)

    def one_testcase(self, testcase: BaseTestCase, namespace: Namespace) -> None:
        print('testcase: %s(%r)' % (testcase.name, namespace.dry_run))
        if namespace.dry_run: return
