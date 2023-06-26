from sooners.utils import Context
from sooners.settings import BaseSettings

class Settings(BaseSettings):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__('zh_Hans', *args, **kwargs)
        self.components.install('sooners.sample1')

    def baseapi_setup(self):
        super().baseapi_setup()
        self.allowed_origins.append('http://localhost:8000')
        self.servers.install('devel', host = '0.0.0.0')

    def model_setup(self):
        super().model_setup()
        self.databases.install_postgresql(
            'test0', 'sooners', 'sooners', '$abc123def$', default_db = True)
        self.databases.install_mysql('test1', 'sooners', 'sooners', '$abc123def$')
        self.databases.install_sqlite3_at_dbs('test2', 'test2.sqlite3')
        index2suffix = lambda index: '%03u' % index
        self.model_params.update(Point = Context(batch_map = dict(
            test0 = tuple(map(index2suffix, range(0, 1))),
            test1 = tuple(map(index2suffix, range(1, 3))),
            test2 = tuple(map(index2suffix, range(3, 6))))))
