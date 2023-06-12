from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Iterable
from ...command import BaseCommand

class Command(BaseCommand):
    help = ('compile all po files into mo files.')
    domain, msgfmt_options = 'sooners', ['--check-format']

    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        self.check_programs('msgfmt')
        for component in self.settings.components.values():
            if not component.is_local or component.locale_dir is None: continue
            pofile = component.locale_dir.joinpath('%s.po' % self.settings.locale_name)
            if not pofile.is_file(): self.prompt1('%s is not exists.' % pofile)
            else:
                for mofile in self.write_mo_file(pofile):
                    self.prompt0('Written: %s' % mofile)

    def write_mo_file(self, pofile: Path) -> Iterable[Path]:
        mofile = pofile.parent.joinpath(pofile.name.stem + '.mo')
        msgs, errors, status = self.popen_with_check(
            'msgfmt', *self.msgfmt_options, '-o', mofile, pofile)
        yield mofile
