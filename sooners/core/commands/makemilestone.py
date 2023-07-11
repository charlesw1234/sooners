from argparse import ArgumentParser, Namespace
from datetime import date
from typing import Iterable
from ...utils import Context
from ...command import BaseCommand

milestone_format = '''from sooners.utils import Context
from sooners.settings import the_settings
from sooners.milestone import Milestone
from sooners.milestone.dbschema import DBSchemaStep

milestone = Milestone(
    %(previous_milestone_name)r,
    DBSchemaStep(
        %(component_model_versions)r,
        %(model_params)r)
)
'''

class Command(BaseCommand):
    help = ('generate a milestone by the current settings.')
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('--previous', type = str, nargs = '?',
                            help = 'specify the name of previous milestone.')

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        self.settings.load_models()
        milestone_dir = self.settings.source_root.joinpath('milestones')
        if namespace.previous is not None: previous_milestone_name = namespace.previous
        else:
            try: previous_milestone_name = max(milestone_dir.glob('*.py')).stem
            except ValueError as exc: previous_milestone_name = None
        for suffix in 'abcdefghijklmnopqrstuvwxyz':
            milestone_file_name = '%s%s.py' % (date.today().strftime('%Y%m%d'), suffix)
            if not milestone_dir.joinpath(milestone_file_name).is_file(): break
        component_model_versions = Context(**dict(self.load_component_model_versions()))
        milestone_file_path = milestone_dir.joinpath(milestone_file_name)
        self.prompt('Writting %s ...' % milestone_file_path)
        with open(milestone_file_path, 'wt') as wfp:
            wfp.write(milestone_format % dict(
                previous_milestone_name = previous_milestone_name,
                component_model_versions = component_model_versions,
                model_params = self.settings.metadata.save_params()))

    def load_component_model_versions(self) -> Iterable[tuple[str, Context]]:
        for component in self.settings.components.values():
            if (xmlversion := self.settings.metadata.make_version(component)) is None: continue
            checksum = xmlversion.getAttribute('checksum')
            xmlversions = component.version_parse_all()
            func = lambda xmlversion: xmlversion.getAttribute('checksum') == checksum
            if not bool(matched_xmlversions := tuple(filter(func, xmlversions))):
                raise RuntimeError('The model version of %r is not saved yet.' % component)
            assert(len(matched_xmlversions) == 1)
            yield (component.name, int(matched_xmlversions[0].getAttribute('version')))
