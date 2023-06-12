from os import environ
from pathlib import Path
from sys import stdout
from sqlalchemy import create_engine, MetaData, Column, String, SmallInteger, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine

class BaseDatabase(object):
    def __init__(self, name: str, default_db: bool = False) -> None:
        self.name, self.default_db = name, default_db
    def __eq__(self, other) -> bool: return self.name == other.name
    def __lt__(self, other) -> bool: return self.name < other.name
    def __le__(self, other) -> bool: return self.name <= other.name
    def __gt__(self, other) -> bool: return self.name > other.name
    def __ge__(self, other) -> bool: return self.name >= other.name
    def __hash__(self): return hash(self.name)
    def __repr__(self) -> str: return 'Database(%r)' % (self.engine.url,)

    def patch_oper(self):
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        conn = self.engine_sync.connect()
        ctx = MigrationContext.configure(conn)
        oper = Operations(ctx)
        assert(not hasattr(oper, 'database'))
        oper.database = self
        return oper
    #def patch_oper(self, as_sql: bool = False,
    #               transaction_ornot: bool = True, output = stdout):
    #    from alembic.ddl.impl import DefaultImpl
    #    impl_cls = DefaultImpl.get_by_dialect(self.engine_sync.dialect)
    #    return impl_cls(self.engine_sync.dialect, self.engine_sync.connect(),
    #                    as_sql, transaction_ornot, output, {})

    def dbshellenv(self) -> dict[str, str] | None: return None

class DatabaseSQLite3(BaseDatabase):
    def __init__(self, name: str, dbpath: Path,
                 dbuser: str | None = None, dbpass: str | None = None,
                 default_db: bool = False) -> None:
        super().__init__(name, default_db)
        if not isinstance(dbpath, Path): raise ValueError('dbpath must be a Path.')
        if dbuser is None and dbpass is None: pass
        elif not isinstance(dbuser, str) or not isinstance(dbpass, str):
            raise ValueError('dbuser and dbpass must be two None or two strings.')
        self.dbpath, self.dbuser, self.dbpass = dbpath, dbuser, dbpass
        if self.dbuser is not None:
            raise NotImplemetned('sqlite3 with user & pass is not supported yet.')
        urlfmt, urldict = '%(dialect)s:///%(path)s', dict(path = self.dbpath.absolute())
        self.engine = create_async_engine(
            urlfmt % dict(dialect = 'sqlite+aiosqlite', **urldict))
        self.engine_sync = create_engine(urlfmt % dict(dialect = 'sqlite', **urldict))

    def dbshell(self) -> tuple[str]:
        if self.dbuser is None: return ('sqlite3', self.dbpath)
        else: raise NotImplemented('sqlite3 with user & pass is not supported yet.')

class DatabaseMySQL(BaseDatabase):
    def __init__(self, name: str, dbname: str, dbuser: str, dbpass: str,
                 dbhost: str | None = None, dbport: int | None = None,
                 default_db: bool = False) -> None:
        super().__init__(name, default_db)
        if dbhost is None: dbhost = 'localhost'
        elif not isinstance(dbhost, str): raise ValueError('dbhost must be a string.')
        if dbport is None: dbport = 3306
        elif not isinstance(dbport, int): raise ValueError('dbport must be an integer.')
        self.dbname, self.dbuser, self.dbpass = dbname, dbuser, dbpass
        self.dbhost, self.dbport = dbhost, dbport
        urlfmt = '%(dialect)s://%(dbuser)s:%(dbpass)s@%(dbhost)s:%(dbport)u/%(dbname)s'
        urldict = dict(dbname = self.dbname, dbuser = self.dbuser, dbpass = self.dbpass,
                       dbhost = self.dbhost, dbport = self.dbport)
        self.engine = create_async_engine(urlfmt % dict(dialect = 'mysql+asyncmy', **urldict))
        self.engine_sync = create_engine(urlfmt % dict(dialect = 'mysql', **urldict))

    def dbshell(self) -> tuple[str]:
        return ('mysql', '--user=%s' % self.dbuser, '--password=%s' % self.dbpass,
                '--host=%s' % self.dbhost, '--port=%u' % self.dbport, self.dbname)

class DatabasePostgreSQL(BaseDatabase):
    def __init__(self, name: str, dbname: str, dbuser: str, dbpass: str,
                 dbhost: str | None = None, dbport: int | None = None,
                 default_db: bool = False) -> None:
        super().__init__(name, default_db)
        if dbhost is None: dbhost = 'localhost'
        elif not isinstance(dbhost, str): raise ValueError('dbhost must be a string.')
        if dbport is None: dbport = 5432
        elif not isinstance(dbport, int): raise ValueError('dbport must be an integer.')
        self.dbname, self.dbuser, self.dbpass = dbname, dbuser, dbpass
        self.dbhost, self.dbport = dbhost, dbport
        urlfmt = '%(dialect)s://%(dbuser)s:%(dbpass)s@%(dbhost)s:%(dbport)u/%(dbname)s'
        urldict = dict(dbname = self.dbname, dbuser = self.dbuser, dbpass = self.dbpass,
                       dbhost = self.dbhost, dbport = self.dbport)
        self.engine = create_async_engine(
            urlfmt % dict(dialect = 'postgresql+asyncpg', **urldict))
        self.engine_sync = create_engine(
            urlfmt % dict(dialect = 'postgresql+psycopg', **urldict))

    def dbshell(self) -> tuple[str]:
        dbhost, dbport = '--host=%s' % self.dbhost, '--port=%u' % self.dbport
        return ('psql', dbhost, dbport, self.dbname, self.dbuser)

    def dbshellenv(self) -> dict[str, str] | None:
        return dict(PGPASSWORD = self.dbpass, TERM = environ['TERM'])
