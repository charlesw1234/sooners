from argparse import Namespace
from gunicorn import debug, util
from gunicorn.config import KNOWN_SETTINGS
from gunicorn.app.base import BaseApplication

class SoonersApplication(BaseApplication):
    def __init__(self, namespace: Namespace, settings, **kws):
        self.namespace, self.settings = namespace, settings
        super().__init__(**kws)
    def load_config(self):
        server_config = self.settings.servers[self.namespace.server]
        host = server_config.get('host', '127.0.0.1')
        port = server_config.get('port', 8000)
        cutoff_fields = ('config', 'wsgi_app', 'host', 'port')
        override_fields = dict(
            bind = [':'.join([host, str(port)]), *server_config.get('bind', ())],
            daemon = True, chdir = self.settings.source_root,
            accesslog = self.settings.logs_dir.joinpath('%s.access' % self.namespace.server),
            errorlog = self.settings.logs_dir.joinpath('%s.error' % self.namespace.server),
            pidfile = self.settings.logs_dir.joinpath('%s.pid' % self.namespace.server),
            tmp_upload_dir = self.settings.sandbox_root.joinpath('uploads'))
        default_fields = dict(
            worker_class = 'uvicorn.workers.UvicornWorker',
            workers = 4, threads = 4, proc_name = self.namespace.server)
        for setting in KNOWN_SETTINGS:
            if setting.name in cutoff_fields: continue
            elif setting.name in override_fields:
                self.cfg.set(setting.name, override_fields[setting.name])
            elif setting.name in default_fields:
                value = server_config.get(setting.name, default_fields[setting.name])
                self.cfg.set(setting.name, value)
            elif setting.name in server_config:
                self.cfg.set(setting.name, server_config[setting.name])
    def load(self): return app_str # self.settings.app
    def run(self):
        if self.cfg.spew: debug.spew()
        if self.cfg.daemon: util.daemonize(self.cfg.enable_stdio_inheritance)
        super().run()
