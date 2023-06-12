if __name__ == '__main__':
    from os import environ
    from pathlib import Path
    from sys import argv, path as syspath
    from sooners.core import execute_from_command_line
    from settings import source_version
    if environ['SOURCE_ROOT'] not in syspath: syspath.insert(0, environ['SOURCE_ROOT'])
    execute_from_command_line(
        Path(environ['SOURCE_ROOT']), Path(environ['SANDBOX_ROOT']), source_version, argv)
