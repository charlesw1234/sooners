from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Iterable
from ...command import BaseCommand
from ...component import BaseComponent

class Command(BaseCommand):
    help = ('collect all i18n strings and merge into po files.')
    domain = 'sooners'
    fnpatterns = ['*.py']
    msgmerge_options = ['-q', '--previous']
    msguniq_options = ['--to-code=utf-8']
    msgattrib_options = ['--no-obsolete']
    xgettext_options = ['--from-code=UTF-8', '--add-comments=Translators']
    ropen = lambda self, fpath: fpath.open('r', encoding = 'utf-8')
    wopen = lambda self, fpath: fpath.open('w', encoding = 'utf-8', newline = '\n')

    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            '--no-wrap', action = 'store_true',
            help = "Don't break long message lines into several lines.")
        parser.add_argument(
            '--no-location', action = 'store_true',
            help = "Don't write '#: filename:line' lines.")
        parser.add_argument(
            '--add-location', choices = ('full', 'file', 'never'),
            const = 'full', nargs = '?',
            help = '...')
        parser.add_argument(
            '--no-obsolete', action = 'store_true',
            help = "Remove obsolete message strings.")

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        if namespace.no_wrap:
            self.msgmerge_options = self.msgmerge_options[:] + ['--no-wrap']
            self.msguniq_options = self.msguniq_options[:] + ['--no-wrap']
            self.msgattrib_options = self.msgattrib_options[:] + ['--no-wrap']
            self.xgettext_options = self.xgettext_options[:] + ['--no-wrap']
        if namespace.no_location:
            self.msgmerge_options = self.msgmerge_options[:] + ['--no-location']
            self.msguniq_options = self.msguniq_options[:] + ['--no-location']
            self.msgattrib_options = self.msgattrib_options[:] + ['--no-location']
            self.xgettext_options = self.xgettext_options[:] + ['--no-location']
        if namespace.add_location:
            arg_add_location = '--add-location=%s' % namespace.add_location
            self.msgmerge_options = self.msgmerge_options[:] + [arg_add_location]
            self.msguniq_options = self.msguniq_options[:] + [arg_add_location]
            self.msgattrib_options = self.msgattrib_options[:] + [arg_add_location]
            self.xgettext_options = self.xgettext_options[:] + [arg_add_location]
        self.no_obsolete = namespace.no_obsolete
        self.check_programs('xgettext', 'msguniq', 'msgmerge', 'msgattrib')
        for component in self.settings.components.values():
            if not component.is_local or component.locale_dir is None: continue
            for potfile in self.write_pot_file(
                    component.locale_dir, self.find_files(component)):
                self.prompt0('Written: %s' % potfile)
                for pofile in self.write_po_file(potfile, self.settings.locale_name):
                    self.prompt0('Written: %s' % pofile)

    def find_files(self, component: BaseComponent) -> list[Path]:
        files = list()
        for fnpattern in self.fnpatterns: files.extend(component.root.rglob(fnpattern))
        return sorted(files)

    def write_pot_file(self, locale_dir: Path, files: list[Path]) -> Iterable[Path]:
        msgs, errors, status = self.popen_with_check(
            'xgettext', '-d', self.domain, '--language=Python',
            '--keyword=_', '--keyword=N_', '--output=-',
            *self.xgettext_options, *files)
        if not msgs: return
        potfile = locale_dir.joinpath('%s.pot' % self.domain)
        self.wopen(potfile).write(msgs)
        msgs, errors, status = self.popen_with_check(
            'msguniq', *self.msguniq_options, potfile)
        self.wopen(potfile).write(msgs)
        yield potfile

    def write_po_file(self, potfile: Path, locale: str) -> Iterable[Path]:
        pofile = potfile.parent.joinpath('%s.po' % locale)
        if not pofile.is_file(): self.wopen(pofile).write(self.ropen(potfile).read())
        else:
            msgs, errors, status = self.popen_with_check(
                'msgmerge', *self.msgmerge_options, pofile, potfile)
            self.wopen(pofile).write(msgs)
        if self.no_obsolete:
            msgs, errors, status = self.popen_with_check(
                'msgattrib', *self.msgattrib_options, '-o', pofile, pofile)
        yield pofile
