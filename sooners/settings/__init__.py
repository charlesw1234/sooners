from importlib import import_module
from pathlib import Path
from xml.dom.minidom import Element, parse
from .. import SourceVersion, source_version as sooners_source_version
from ..utils import SmartContext, SettingsMap
from .model import SettingsModelMixin
from .fastapi import SettingsFastAPIMixin

the_settings = None

class ComponentMap(SettingsMap):
    def install(self, component_name: str, *args, **kwargs) -> object:
        module = import_module(component_name, component_name)
        component = module.Component(module, self.settings, *args, **kwargs)
        return super().install(component, component)

    def locate_component(self, path: Path):
        for component in self.values():
            if component.path_in(path): return component
        return None

class BaseSettings(SettingsModelMixin, SettingsFastAPIMixin):
    def __init__(self, locale_name: str, source_root: Path, sandbox_root: Path,
                 source_version: SourceVersion, **kwargs) -> None:
        self.locale_name, self.translations = locale_name, None
        self.source_version = source_version
        self.sooners_source_version = sooners_source_version
        self.python_suffixes = ('.py', '.pyc', '.pyo', '.pyd')
        self.stem_name = self.__module__.split('.')[-1]
        self.source_root, self.sandbox_root = source_root, sandbox_root
        self.logs_dir = sandbox_root.joinpath('logs')
        self.closed_logs_dir = sandbox_root.joinpath('closed.logs')
        self.log_limit = 256 * 1024 * 1024
        self.components = ComponentMap(self)
        self.components.install('sooners.core')

    def post_setup(self):
        global the_settings
        if the_settings is not None: raise RuntimeError('Multiple settings initialized!')
        the_settings = self
        return self

    def __repr__(self) -> str: return 'Settings(%s)' % self.__module__

    def is_python(self, filename: str) -> bool:
        return any(map(lambda suffix: filename.endswith(suffix), self.python_suffixes))

    def prepend_locale_dir(self, locale_dir: Path) -> None:
        if not (mofile := locale_dir.joinpath('%s.mo' % self.locale_name)).is_file(): return
        from gettext import GNUTranslations
        translations = GNUTranslations(mofile.open('rb'))
        if self.translations is not None: translations.add_fallback(self.translations)
        self.translations = translations

def _check_settings_at(source_root: Path, *module_pathes: tuple[str]) -> type | None:
    module_name = '.'.join(['settings', *module_pathes])
    try: module = import_module(module_name, module_name)
    except ImportError as exc: return None
    return getattr(module, 'Settings', None)

def locate_settings(source_root: Path, sandbox_root: Path,
                    source_version: SourceVersion) -> BaseSettings:
    from getpass import getuser
    from socket import gethostname
    host, user = gethostname(), getuser()
    host = host.replace('-', '_').replace('.', '_')
    # for production settings.
    if (settings_class := _check_settings_at(source_root, host, user)) is not None: pass
    elif (settings_class := _check_settings_at(source_root, host)) is not None: pass
    elif (settings_class := _check_settings_at(source_root, 'anyhost', user)) is not None: pass
    # for development settings.
    elif (settings_class := _check_settings_at(source_root, 'devel')) is not None: pass
    else: raise ImportError('Unable to locate any available settings.')
    global the_settings
    the_settings = settings_class(source_root, sandbox_root, source_version).post_setup()
    return the_settings

def reset_settings(settings: BaseSettings) -> None:
    global the_settings
    the_settings = settings
