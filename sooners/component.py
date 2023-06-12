from glob import glob
from importlib import import_module
from os import makedirs, scandir
from pathlib import Path
from typing import Iterable
from xml.dom.minidom import Element, parse
from .utils import Context

class BaseComponent(object):
    def __init__(self, module, settings) -> None:
        self._cached_versions, self._cached_patches = dict(), dict()
        self.module, self.settings = module, settings
        self.name = self.module.__name__.replace('.', '_')
        self.root = Path(self.module.__path__[0])
        self.root_parts = self.root.absolute().parts
        source_root_parts = self.settings.source_root.absolute().parts
        self.is_local = self.root_parts[:len(source_root_parts)] == source_root_parts
        self.is_sys = not self.is_local
        self.history_dir = self.root.joinpath('history')
        self.locale_dir = self.root.joinpath('locale')
        settings.prepend_locale_dir(self.locale_dir)

    def __repr__(self) -> str: return '%s(%s)' % (self.__class__.__name__, self.name)
    def __eq__(self, other) -> bool: return self.name == other.name

    def path_in(self, path: Path) -> bool:
        return path.absolute().parts[:len(self.root_parts)] == self.root_parts

    def load_models(self, settings) -> None:
        models_module_name = '%s.models' % self.module.__name__
        try: import_module(models_module_name, models_module_name)
        except ModuleNotFoundError as exc: pass

    def load_statics(self) -> None:
        if not self.root.joinpath('statics').is_dir(): return
        from fastapi.staticfiles import StaticFiles
        self.settings.app.mount(
            '/%s/statics' % self.name,
            StaticFiles(directory = self.root.joinpath('statics')),
            name = '%s.statics' % self.name)

    def version_fname(self, version: int) -> str:
        return 'version.%04u.xml' % version
    def version_fpath(self, version: int) -> str:
        return self.history_dir.joinpath(self.version_fname(version))
    def version_parse(self, version: int) -> Element:
        if version not in self._cached_versions:
            with self.version_fpath(version).open('rt') as rfp:
                self._cached_versions[version] = parse(rfp).documentElement
        return self._cached_versions[version]
    def version_parse_all(self) -> tuple[Element]:
        fpathes = sorted(self.history_dir.glob('version.*.xml'))
        func0 = lambda fpath: int(fpath.name.split('.')[1])
        func1 = lambda version: self.version_parse(version)
        return tuple(map(func1, map(func0, fpathes)))
    def version_write(self, xmlversion: Element, version: int) -> Element:
        xmlversion.setAttribute('sooners', repr(self.settings.sooners_source_version))
        xmlversion.setAttribute('component', self.name)
        xmlversion.setAttribute('version', '%04u' % version)
        if not self.history_dir.is_dir(): makedirs(self.history_dir)
        version_fpath = self.version_fpath(version)
        xmlversion_text = xmlversion.ownerDocument.toprettyxml(indent = '  ')
        open(version_fpath, 'wt', encoding = 'utf-8').write(xmlversion_text)
        return xmlversion

    def patch_fname(self, version0: int, version1: int) -> str:
        return 'patch.%04u.%04u.xml' % (version0, version1)
    def patch_fpath(self, version0: int, version1: int) -> str:
        return self.history_dir.joinpath(self.patch_fname(version0, version1))
    def patch_parse(self, version0: int, version1: int) -> Element:
        if (version0, version1) not in self._cached_patches:
            with self.patch_fpath(version0, version1).open('rt') as rfp:
                self._cached_patches[(version0, version1)] = parse(rfp).documentElement
        return self._cached_patches[(version0, version1)]
    def patch_write(self, xmlpatch: Element, version0: int, version1: int) -> Element:
        xmlpatch.setAttribute('sooners', repr(self.settings.sooners_source_version))
        xmlpatch.setAttribute('component', self.name)
        xmlpatch.setAttribute('version0', '%04u' % version0)
        xmlpatch.setAttribute('version1', '%04u' % version1)
        if not self.history_dir.is_dir(): makedirs(self.history_dir)
        patch_fpath = self.patch_fpath(version0, version1)
        xmlpatch_text = xmlpatch.ownerDocument.toprettyxml(indent = '  ')
        open(patch_fpath, 'wt', encoding = 'utf-8').write(xmlpatch_text)
        return xmlpatch

class BaseComponentScan(object):
    @classmethod
    def make_map_name(cls, component: BaseComponent, name: str) -> str:
        # can be overrided to change its rules.
        return '%s/%s' % (component.name, name)

class BaseComponentFilePyClass(BaseComponentScan):
    @classmethod
    def scan_component(cls, component: BaseComponent) -> Iterable[Context]:
        component_name, filename = component.module.__name__, cls.component_filename
        module_name = '%s.%s' % (component_name, filename)
        try: module = import_module(module_name, module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name: return
            elif exc.name == component_name: return
            else: raise exc
        for member_name, member_class in module.__dict__.items():
            if not isinstance(member_class, type): continue
            if not issubclass(member_class, cls): continue
            if member_class is cls: continue
            yield Context(component = component, name = member_name, klass = member_class)

class BaseComponentSubDirPyClass(BaseComponentScan):
    @classmethod
    def load_from_subdir(cls, object_name: str, component: BaseComponent) -> type:
        component_name, subdir = component.module.__name__, cls.component_subdir
        module_name = '%s.%s.%s' % (component_name, subdir, object_name)
        try: module = import_module(module_name, module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name: return None
            elif exc.name == '%s.%s' % (component_name, subdir): return None
            elif exc.name == component_name: return None
            else: raise exc
        if not hasattr(module, cls.component_object_name): return None
        object_class = getattr(module, cls.component_object_name)
        if not issubclass(object_class, cls): return None
        return object_class

    @classmethod
    def importable_names(cls, component: BaseComponent) -> Iterable[str]:
        from .settings import the_settings
        if (subdir := component.root.joinpath(cls.component_subdir)).is_dir():
            for direntry in subdir.iterdir():
                if direntry.name in ('__pycache__', '__init__.py'): continue
                if direntry.is_dir(): yield direntry.name
                elif direntry.is_file() and the_settings.is_python(direntry.name):
                    yield direntry.stem

    @classmethod
    def scan_component(cls, component: BaseComponent) -> Iterable[Context]:
        component_name, subdir = component.module.__name__, cls.component_subdir
        for name in sorted(cls.importable_names(component)):
            module_name = '%s.%s.%s' % (component_name, subdir, name)
            try: module = import_module(module_name, module_name)
            except ModuleNotFoundError as exc:
                if exc.name == module_name: continue
                elif exc.name == '%s.%s' % (component_name, subdir): continue
                elif exc.name == component_name: continue
                else: raise exc
            if not hasattr(module, cls.component_object_name): continue
            object_class = getattr(module, cls.component_object_name)
            if not issubclass(object_class, cls): continue
            yield Context(component = component, name = name, klass = object_class)
