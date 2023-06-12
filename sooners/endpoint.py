from typing import Iterable
from fastapi.routing import APIRouter
from .component import BaseComponentFilePyClass

class EPVersion(object):
    @classmethod
    def parse(cls, version_str: str):
        major_str, minor_str = version_str.split('.')
        major = None if major_str in ('*', 'xx') else int(major)
        minor = None if minor_str in ('*', 'xx') else int(minor)
        return cls(major, minor)

    def __init__(self, major: int | None = None, minor: int | None = None) -> None:
        self.major, self.minor = major, minor
    def __hash__(self) -> int: return hash((self.major, self.minor))
    def __repr__(self) -> str:
        return 'v-%s.%s' % (
            'xx' if self.major is None else '%02u' % self.major,
            'xx' if self.minor is None else '%02u' % self.minor)
    def pattern_ornot(self) -> bool: return None in (self.major, self.minor)
    def match(self, version) -> bool:
        if self.major is not None and self.major != version.major: return False
        if self.minor is not None and self.minor != version.minor: return False
        return True

class BaseEndpoint(BaseComponentFilePyClass):
    component_filename = 'endpoints'
    versions = (EPVersion(),)
    class exctype(Exception): pass

    @classmethod
    def get_versions(cls) -> Iterable[EPVersion]:
        for version in cls.versions:
            if isinstance(version, EPVersion): yield version
            elif isinstance(version, str): yield EPVersion.parse(version)
            else: raise ValueError('Invalid version %r encountered.' % version)

    @classmethod
    def endpoint_setup(cls, apirouter: APIRouter) -> None:
        for method in ('get', 'put', 'post'):
            if not callable(method_func := getattr(cls, method, None)): continue
            apirouter.add_api_route(
                '/%s' % cls.path, method_func,
                response_model = method_func.__annotations__['return'],
                methods= [method.upper()])
        for method in ('websocket',):
            if not callable(method_func := getattr(cls, method, None)): continue
            apirouter.add_api_websocket_route('/%s' % cls.path, method_func)
