#from jose import jwt
from ..utils import Arguments, DefaultDict
from .baseapi import SettingsBaseAPIMixin

class SettingsFastAPIMixin(SettingsBaseAPIMixin):
    def baseapi_setup(self) -> None:
        super().baseapi_setup()
        self._fastapi_arguments = Arguments()

    @property
    def fastapi_arguments(self):
        if not hasattr(self, '_fastapi_arguments'): self.baseapi_setup()
        return self._fastapi_arguments

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
        default_cn2ar = v2cn2ar[EPVersion(
            self.source_version.major, self.source_version.minor)]
        for component in self.components.values():
            for epctx in BaseEndpoint.scan_component(component):
                for version in epctx.klass.get_versions():
                    if not version.match(self.source_version): pass
                    elif version.pattern_ornot(): pattern_endpoints.append(epctx)
                    else: epctx.klass.endpoint_setup(v2cn2ar[version][component.name])
        for version, cn2ar in v2cn2ar.items():
            for epctx in pattern_endpoints:
                for epver in epctx.klass.get_versions():
                    if epver.pattern_ornot() and epver.match(version):
                        epctx.klass.endpoint_setup(cn2ar[epctx.component.name])
            for apirouter in cn2ar.values(): self._app.include_router(apirouter)
        return self
