from argparse import ArgumentParser, Namespace
from os import kill
from signal import SIGHUP, SIGINT
from ...command import BaseCommand

app_str = 'sooners.serve:the_settings.app'
class Command(BaseCommand):
    help = ('run the development/production server.')
    def add_arguments(self, parser: ArgumentParser):
        super().add_arguments(parser)
        parser.add_argument('--show', action = 'store_true', help = '')
        helpstr = 'start development server or start/reload/stop production server.'
        parser.add_argument('operation', help = helpstr,
                            choices = ('devel', 'start', 'reload', 'stop'))
        parser.add_argument('server', help = 'the server to be used.',
                            choices = self.settings.servers.keys())

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        getattr(self, 'on_%s' % namespace.operation)(namespace)

    def on_devel(self, namespace: Namespace) -> None:
        self.show(namespace)
        server_config = self.settings.servers[namespace.server]
        host = server_config.get('host', '127.0.0.1')
        port = server_config.get('port', 8000)
        gunicorn2uvicorn = dict(
            keyfile = 'ssl_keyfile', certfile = 'ssl_certfile', ssl_version = 'ssl_version',
            cert_reqs = 'ssl_cert_reqs', ca_certs = 'ssl_ca_certs', ciphers = 'ssl_ciphers')
        func0 = lambda guni2uvi: guni2uvi[0] in server_config
        func1 = lambda guni2uvi: (guni2uvi[1], server_config[guni2uvi[0]])
        from uvicorn import run as uvicorn_run
        uvicorn_run(app_str, host = host, port = port,
                    reload = True, reload_dirs = [self.settings.source_root],
                    **dict(map(func1, filter(func0, gunicorn2uvicorn.items()))))

    def on_start(self, namespace: Namespace):
        self.show(namespace)
        from .libs.server import SoonersApplication
        SoonersApplication(namespace, self.settings).run()

    def on_reload(self, namespace: Namespace) -> None:
        pidfile = self.settings.logs_dir.joinpath('%s.pid' % namespace.server)
        kill(pid := int(pidfile.open('rt').read()), SIGHUP)

    def on_stop(self, namespace: Namespace) -> None:
        pidfile = self.settings.logs_dir.joinpath('%s.pid' % namespace.server)
        kill(pid := int(pidfile.open('rt').read()), SIGINT)

    def show(self, namespace: Namespace) -> None:
        if not namespace.show: return
        prompt_format = '%%%us:%%%us: %%s' % (
            max(map(lambda route: len(route.__class__.__name__), self.settings.app.routes)),
            max(map(lambda route: len(route.name), self.settings.app.routes)))
        for route in self.settings.app.routes:
            self.prompt(prompt_format % (route.__class__.__name__, route.name, route.path))
        if hasattr(self.settings, '_templates'):
            for dirpath in self.settings._templates.env.loader.searchpath:
                self.prompt('template search path: %r' % dirpath)
