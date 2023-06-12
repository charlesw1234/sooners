from ..utils import SettingsMap

class ServersMap(SettingsMap):
    def install(self, server_name: str, **server_config):
        self[server_name] = server_config
        setattr(self, server_name, server_config)

class SettingsBaseAPIMixin(object):
    def baseapi_setup(self) -> None:
        self._allowed_origins = list()
        self._servers = ServersMap(self)

    @property
    def allowed_origins(self):
        if not hasattr(self, '_allowed_origins'): self.baseapi_setup()
        return self._allowed_origins

    @property
    def servers(self):
        if not hasattr(self, '_servers'): self.baseapi_setup()
        return self._servers
