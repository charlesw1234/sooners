from typing import Iterable
from fastapi.routing import APIRouter
from .utils import ServeVersion
from .component import BaseComponentFilePyClass

class EPVersionPart(object):
    def __init__(self, version_part_str: str) -> None:
        if version_part_str in ('*', 'xx'): self.value, self.oper = None, None
        elif version_part_str.endswith('+'):
            self.value, self.oper = int(version_part_str[:-1]), '+'
        elif version_part_str.endswith('-'):
            self.value, self.oper = int(version_part_str[:-1]), '-'
        else: self.value, self.oper = int(version_part_str), '='
    def __repr__(self) -> str:
        if self.oper is None: return 'xx'
        elif self.oper in '=': return '%u' % self.value
        elif self.oper in '+-': return '%u%s' % (self.value, self.oper)
        else: raise ValueError('Unsupported EPVersionPart value: (%r, %r).' %
                               (self.value, self.oper))
    def match(self, value: int) -> bool:
        if self.oper is None: return True
        elif self.oper == '=' and self.value == value: return True
        elif self.oper == '+' and self.value <= value: return True
        elif self.oper == '-' and self.value >= value: return True
        else: return False

class EPVersion(object):
    def __init__(self, version_str: str = '*.*') -> None:
        major_str, minor_str = version_str.split('.')
        self.major, self.minor = EPVersionPart(major_str), EPVersionPart(minor_str)
    def __repr__(self) -> str: return 'EPVersion(%r.%r)' % (self.major, self.minor)
    def match(self, version: ServeVersion) -> bool:
        return self.major.match(version.major) and self.minor.match(version.minor)

class BaseEndpoint(BaseComponentFilePyClass):
    component_filename = 'endpoints'
    versions = (EPVersion(),)
    class exctype(Exception): pass

    @classmethod
    def get_versions(cls) -> Iterable[EPVersion]:
        for version in cls.versions:
            if isinstance(version, EPVersion): yield version
            elif isinstance(version, str): yield EPVersion(version)
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
