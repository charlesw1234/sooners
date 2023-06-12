from pathlib import Path
from ..utils import Context, DefaultDict, SettingsMap

class DatabaseMap(SettingsMap):
    def install_sqlite3(self, database_name: str, dbpath: Path,
                        dbuser: str | None = None, dbpass: str | None = None,
                        default_db: bool = False):
        from ..db.database import DatabaseSQLite3
        database = DatabaseSQLite3(database_name, dbpath, dbuser, dbpass, default_db)
        return super().install(database, database)

    def install_sqlite3_at_dbs(self, database_name: str, dbfilename: str,
                               dbuser: str | None = None, dbpass: str | None = None,
                               default_db: bool = False):
        dbpath = self.settings.sandbox_root.joinpath('dbs', dbfilename)
        return self.install_sqlite3(database_name, dbpath, dbuser, dbpass, default_db)

    def install_mysql(self, database_name: str, dbname: str, dbuser: str, dbpass: str,
                      dbhost: str | None = None, dbport: int | None = None,
                      default_db: bool = False):
        from ..db.database import DatabaseMySQL
        database = DatabaseMySQL(database_name, dbname, dbuser, dbpass,
                                 dbhost, dbport, default_db)
        return super().install(database, database)

    def install_postgresql(self, database_name: str, dbname: str, dbuser: str, dbpass: str,
                           dbhost: str | None = None, dbport: int | None = None,
                           default_db: bool = False):
        from ..db.database import DatabasePostgreSQL
        database = DatabasePostgreSQL(database_name, dbname, dbuser, dbpass,
                                      dbhost, dbport, default_db)
        return super().install(database, database)

class SettingsModelMixin(object):
    def model_setup(self) -> None:
        self._model_params = Context()
        self._databases = DatabaseMap(self)
        from ..db.metadata import MetaData
        from ..db.registry import Registry
        self._metadata = MetaData(self)
        self._registry = Registry(metadata = self._metadata)
        #self.load_models()

    @property
    def model_params(self):
        if not hasattr(self, '_model_params'): self.model_setup()
        return self._model_params

    @property
    def databases(self):
        if not hasattr(self, '_databases'): self.model_setup()
        return self._databases

    @property
    def default_database_names(self) -> set[str]:
        if not hasattr(self, '_default_database_names'):
            func0 = lambda database: database.default_db
            func1 = lambda database: database.name
            self._default_database_names = set(
                map(func1, filter(func0, self.databases.values())))
            # only one default database is permitted.
            assert(len(self._default_database_names) == 1)
        return self._default_database_names

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'): self.model_setup()
        return self._metadata

    @property
    def registry(self):
        if not hasattr(self, '_registry'): self.model_setup()
        return self._registry

    def load_models(self):
        for component in self.components.values(): component.load_models(self)
        self.metadata.models.setup(self.model_params, self.databases)
        return self

    def make_migrate_context(self, **kwargs: dict[str, object]) -> Context:
        from sqlalchemy import inspect
        from sqlalchemy.orm import sessionmaker
        func = lambda database_name: self.databases[database_name]
        inspector_func = lambda dbname: inspect(func(dbname).engine_sync)
        operator_func = lambda dbname: func(dbname).patch_oper()
        session_func = lambda dbname: sessionmaker(bind = func(dbname).engine_sync)()
        return Context(settings = self, **kwargs,
                       inspectors = DefaultDict(inspector_func),
                       operators = DefaultDict(operator_func),
                       sessions = DefaultDict(session_func))
