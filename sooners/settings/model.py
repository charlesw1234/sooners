from pathlib import Path
from typing import Iterable
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

class MigrateContextDefault(object):
    def __init__(self, context: Context, default_database_name: str) -> None:
        self.context, self.default_database_name = context, default_database_name
    def __repr__(self) -> str:
        parts = []
        if self.default_database_name in self.context.inspectors: parts.append('inspector')
        if self.default_database_name in self.context.operators: parts.append('operators')
        if self.default_database_name in self.context.sessions: parts.append('sessions')
        if not parts: return '%s(%s)' % (self.__class__.__name__, self.default_database_name)
        return '%s(%s:%s)' % (
            self.__class__.__name__, self.default_database_name, ','.join(parts))
    @property
    def inspector(self):
        return self.context.inspectors[self.default_database_name]
    @property
    def operator(self):
        return self.context.operators[self.default_database_name]
    @property
    def session(self):
        return self.context.sessions[self.default_database_name]

class SettingsModelMixin(object):
    def model_setup(self) -> None:
        self._model_params = Context()
        self._databases = DatabaseMap(self)
        from ..db.metadata import MetaData
        from ..db.registry import Registry
        self._metadata = MetaData(self)
        self._registry = Registry(metadata = self._metadata)

    @property
    def model_params(self):
        if not hasattr(self, '_model_params'): self.model_setup()
        return self._model_params

    @property
    def databases(self):
        if not hasattr(self, '_databases'): self.model_setup()
        return self._databases

    @property
    def default_database_name(self) -> str:
        if not hasattr(self, '_default_database_name'):
            func0 = lambda database: database.default_db
            func1 = lambda database: database.name
            default_database_names = tuple(
                map(func1, filter(func0, self.databases.values())))
            # only one default database is permitted.
            assert(len(default_database_names) == 1)
            self._default_database_name = default_database_names[0]
        return self._default_database_name

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'): self.model_setup()
        return self._metadata

    @property
    def registry(self):
        if not hasattr(self, '_registry'): self.model_setup()
        return self._registry

    def load_models(self):
        if not hasattr(self, '_load_models_done'):
            self._load_models_done = True
            for component in self.components.values(): component.load_models(self)
            self.metadata.models.setup(self.model_params, self.databases)
        return self

    def check_models(self) -> Iterable[object]:
        self.load_models()
        from ..core.models import DBSchemaVersion
        context = self.make_db_context()
        default_dict = DBSchemaVersion.load_default_dict(context)
        func = lambda version_record: version_record.checksum0 != version_record.checksum1
        if any(map(func, default_dict.values())): return False
        for component in self.components.values():
            if component.name not in default_dict:
                if self.metadata.make_version(component) is None: pass
                else: yield component
            elif default_dict[component.name].checksum0 !=\
                 default_dict[component.name].checksum1:
                yield component
            elif (version_dom := self.metadata.make_version(component)) is None:
                yield component
            elif version_dom.getAttribute('checksum') !=\
                 default_dict[component.name].checksum0:
                yield component
            else: pass

    def make_db_context(self, **kwargs: dict[str, object]) -> Context:
        from sqlalchemy import inspect
        from sqlalchemy.orm import sessionmaker
        func = lambda database_name: self.databases[database_name]
        inspector_func = lambda dbname: inspect(func(dbname).engine_sync)
        operator_func = lambda dbname: func(dbname).patch_oper()
        session_func = lambda dbname: sessionmaker(bind = func(dbname).engine_sync)()
        context = Context(settings = self, **kwargs, default = None,
                          inspectors = DefaultDict(inspector_func),
                          operators = DefaultDict(operator_func),
                          sessions = DefaultDict(session_func))
        context.default = MigrateContextDefault(context, self.default_database_name)
        return context

    def make_migrate_context(self, **kwargs: dict[str, object]) -> Context:
        return self.make_db_context(**kwargs)
