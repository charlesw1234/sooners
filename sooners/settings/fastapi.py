#from jose import jwt
from ..utils import Arguments, DefaultDict, ServeVersion
from .baseapi import SettingsBaseAPIMixin

class SettingsFastAPIMixin(SettingsBaseAPIMixin):
    def baseapi_setup(self) -> None:
        super().baseapi_setup()
        self._serve_versions = { ServeVersion(self.source_version.major,
                                              self.source_version.minor) }
        self._fastapi_arguments = Arguments()

    @property
    def fastapi_arguments(self):
        if not hasattr(self, '_fastapi_arguments'): self.baseapi_setup()
        return self._fastapi_arguments

    @property
    def serve_versions(self):
        if not hasattr(self, '_serve_versions'): self.baseapi_setup()
        return self._serve_versions

    @property
    def app(self):
        if not hasattr(self, '_app'):
            from fastapi import FastAPI
            self._app = self.fastapi_arguments(FastAPI)
            self.static_setup().endpoint_setup()
        return self._app

    def static_setup(self):
        if static_dirpath := self.source_root.joinpath('statics').is_dir():
            from fastapi.staticfiles import StaticFiles
            static_files = StaticFiles(directory = static_dirpath)
            self._app.mount('/statics', static_files, name = 'statics')
        for component in self.components.values(): component.load_statics()
        return self

    def endpoint_setup(self):
        from fastapi.routing import APIRouter
        from ..endpoint import EPVersion, BaseEndpoint
        pattern_endpoints = list()
        v2cn2ar = DefaultDict(
            lambda version: DefaultDict(
                lambda component_name: APIRouter(
                    prefix = '/%r/%s' % (version, component_name))))
        for component in self.components.values():
            for epctx in BaseEndpoint.scan_component(component):
                for serve_version in self.serve_versions:
                    func = lambda endpoint_version: endpoint_version.match(serve_version)
                    if not any(map(func, epctx.klass.get_versions())): continue
                    epctx.klass.endpoint_setup(v2cn2ar[serve_version][component.name])
        for serve_version, cn2ar in v2cn2ar.items():
            for apirouter in cn2ar.values(): self._app.include_router(apirouter)
        return self
