from argparse import ArgumentParser, Namespace
from asyncio import gather, get_event_loop
from difflib import get_close_matches
from importlib import import_module
from os import get_exec_path
from pathlib import Path
from readline import parse_and_bind, set_completer_delims, set_completer
from subprocess import PIPE, run
from .termcolors import colorize
from .component import BaseComponentSubDirPyClass

parse_and_bind('tab: complete')
set_completer_delims('')

class BaseCommand(BaseComponentSubDirPyClass):
    component_subdir = 'commands'
    component_object_name = 'Command'
    class exctype(Exception): pass
    help = 'not available'
    def __init__(self, settings, *argv) -> None:
        self.settings = settings
        print('Load <%s@%s>.' % (self.__module__, self.settings.__module__))

    def __repr__(self) -> str:
        return '%s(%s)' % (self.__class__.__name__, self.__module__)

    def make_parser(self) -> ArgumentParser:
        parser = ArgumentParser(description = self.help)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            '-v', '--verbosity', type = int, default = 1, choices = [0, 1, 2, 3],
            help = 'Verbosity level: 0~3')

    def handle(self, namespace: Namespace) -> None:
        self.verbosity = namespace.verbosity

    def async_handle(self, namespace: Namespace) -> None:
        coroutine = gather(*self.make_coroutines(namespace))
        get_event_loop().run_until_complete(coroutine)

    def check_programs(self, *programs) -> None:
        for program in programs:
            func = lambda execdir: Path(execdir).joinpath(program).is_file()
            if any(filter(func, get_exec_path())): continue
            raise self.exctype('Can not locate %r.' % program)

    def popen(self, *args, stdout_encoding = 'utf-8'):
        try: pipe = run(args, stdout = PIPE, stderr = PIPE)
        except OSError as exc: raise self.exctype('Error executing %r' % args) from exc
        return (pipe.stdout.decode(stdout_encoding),
                pipe.stderr.decode(stdout_encoding, errors = 'replace'),
                pipe.returncode)
    def popen_with_check(self, *args, stdout_encoding: str = 'utf-8'):
        stdout, errors, status = self.popen(*args, stdout_encoding = stdout_encoding)
        if errors:
            if status > 0: raise self.exctype('Error executing %r' % args)
            else: self.prompt1(errors)
        return stdout, errors, status

    def prompt(self, message: str, **kwargs) -> None:
        if kwargs: message = colorize(message, **kwargs)
        print(message)
    def emprompt(self, message: str) -> None:
        print(message, opts = ('bold',))

    def prompt0(self, message: str, **kwargs) -> None:
        if self.verbosity >= 0: self.prompt(message, **kwargs)
    def prompt1(self, message: str, **kwargs) -> None:
        if self.verbosity >= 1: self.prompt(message, **kwargs)
    def prompt2(self, message: str, **kwargs) -> None:
        if self.verbosity >= 2: self.prompt(message, **kwargs)
    def prompt3(self, message: str, **kwargs) -> None:
        if self.verbosity >= 3: self.prompt(message, **kwargs)

class CommandCompleter(object):
    def __init__(self, matches: list[str]) -> None:
        self.matches = matches
    def __call__(self, text, state) -> str:
        func0 = lambda match: match.startswith(text)
        func1 = lambda match: match[len(text): ]
        candidates = tuple(map(func1, filter(func0, self.matches)))
        if state >= len(candidates): return None
        return '%s%s' % (text, candidates[state])

def locate_command(settings, command_name: str, *argv) -> BaseCommand:
    for component in settings.components.values():
        command_class = BaseCommand.load_from_subdir(command_name, component)
        if command_class is not None: return command_class(settings, *argv)
    name2class = dict()
    func = lambda ctx: (ctx.name, ctx.klass)
    for component in settings.components.values():
        name2class.update(map(func, BaseCommand.scan_component(component)))
    matches = get_close_matches(command_name, name2class.keys())
    if not matches: raise ImportError('Unable to locate command: %r.' % command_name)
    set_completer(CommandCompleter(matches))
    print('Did you mean: %r ?' % matches)
    new_command_name = input('Command: ').strip()
    if new_command_name not in name2class: raise exc
    return name2class[new_command_name](settings, *argv)
