from os import environ
from pathlib import Path
from settings import source_version
from .settings import locate_settings, the_settings

if the_settings is None:
    the_settings = locate_settings(
        Path(environ['SOURCE_ROOT']), Path(environ['SANDBOX_ROOT']), source_version)
    the_settings.app # touch app property.
