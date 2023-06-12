from argparse import ArgumentParser, Namespace
from importlib import import_module
from ....utils import Context
from ....command import BaseCommand

class BaseMilestoneCommand(BaseCommand):
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('--show', action = 'store_true')
        parser.add_argument('--confirm', action = 'store_true')
        parser.add_argument('--no-action', action = 'store_true')
        parser.add_argument('milestone', type = str)
        parser.add_argument('patterns', nargs = '*', type = str)

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        if not namespace.patterns: namespace.patterns.append('*')
        module = import_module('milestones.%s' % namespace.milestone)
        if namespace.show:
            for line in module.milestone.show(): print(line)
        else:
            from ....milestone import MilestoneStepPatterns
            context = self.settings.make_migrate_context(
                milestone = module.milestone, do_action = not namespace.no_action,
                exctype = self.exctype, prompt = self.prompt)
            patterns = MilestoneStepPatterns(namespace.confirm, *namespace.patterns)
            self.sub_handle(module.milestone, patterns, context)
