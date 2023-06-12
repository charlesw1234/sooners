from pathlib import Path
from os import chdir
from .. import SourceVersion
from ..component import BaseComponent
from ..settings import locate_settings
from ..command import locate_command

class Component(BaseComponent): pass

def execute_from_command_line(
        source_root: Path, sandbox_root: Path,
        source_version: SourceVersion, argv) -> None:
    chdir(source_root)
    settings = locate_settings(source_root, sandbox_root, source_version)
    if argv[1:]: command = locate_command(settings, argv[1])
    else:
        from .commands.help import Command
        command = Command(settings)
    command.handle(command.make_parser().parse_args(argv[2:]))
