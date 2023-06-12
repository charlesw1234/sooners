from base64 import urlsafe_b64encode
from collections import OrderedDict
from datetime import date, datetime, timedelta
from hashlib import sha3_384
from pathlib import Path
from xml.dom.minidom import Element

class Hasher(object):
    checksum_class = sha3_384
    checksum_size = len(urlsafe_b64encode(checksum_class().digest()).decode('ascii'))
    def __init__(self, body: str) -> None:
        self.hasher = self.checksum_class(body.encode('utf-8'))
    def b64digest(self) -> str:
        return urlsafe_b64encode(self.hasher.digest()).decode('ascii')

class Context(object):
    def __init__(self, **name2values: dict[str, object]) -> None:
        self.__names__ = list()
        self.update(**name2values)
    def __bool__(self) -> bool: return bool(self.__names__)
    def __repr__(self) -> str:
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            map(lambda name: '%s=%r' % (name, getattr(self, name)),
                sorted(self.__names__))))
    def update(self, **name2values):
        assert(all(map(lambda name: not hasattr(self, name), name2values.keys())))
        assert(all(map(lambda name: name not in self.__names__, name2values.keys())))
        for name, value in name2values.items(): setattr(self, name, value)
        self.__names__.extend(name2values.keys())
        return self
    def delete(self, *names):
        for name in names: delattr(self, name)
        self.__names__ = list(filter(lambda name: name not in names, self.__names__))
        return self
    def to_dict(self) -> dict[str, object]:
        return dict(map(lambda name: (name, getattr(self, name)), self.__names__))
    def to_dict_deep(self) -> dict[str, object]:
        func0 = lambda name: (name, getattr(self, name))
        func1 = lambda nv: (nv[0], nv[1] if not isinstance(nv[1], self.__class__)
                            else nv[1].to_dict_deep())
        return dict(map(func1, map(func0, self.__names__)))

class SmartContext(object):
    date_format, at_format = '%Y/%m/%d', '%Y/%m/%d-%H:%M:%S'
    def __init__(self, name2values: dict[str, object]):
        self.__names__ = tuple(name2values.keys())
        for name, value in name2values.items():
            suffix = name.split('_')[-1] if '_' in name else None
            def smart_value(value):
                if suffix is not None and isinstance(value, str) and\
                   callable(member := getattr(self, 'str_suffix_%s' % suffix, None)):
                    return member(value)
                elif suffix is not None and isinstance(value, int) and\
                     callable(member := getattr(self, 'int_suffix_%s' % suffix, None)):
                    return member(value)
                elif isinstance(value, tuple): return tuple(map(smart_value, value))
                elif isinstance(value, list): return list(map(smart_value, value))
                return value
            value = SmartContext(value) if isinstance(value, dict) else smart_value(value)
            setattr(self, name, value)
    def __repr__(self) -> str:
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            map(lambda name: '%s=%r' % (name, getattr(self, name)),
                sorted(self.__names__))))

    def str_suffix_date(self, value: str) -> date:
        return datetime.strptime(value, self.date_format).date()
    def str_suffix_at(self, value: str) -> datetime:
        return datetime.strptime(value, self.at_format)
    def int_suffix_seconds(self, value: int) -> timedelta: return timedelta(seconds = value)
    def int_suffix_minutes(self, value: int) -> timedelta: return timedelta(minutes = value)
    def int_suffix_hours(self, value: int) -> timedelta: return timedelta(hours = value)
    def int_suffix_days(self, value: int) -> timedelta: return timedelta(days = value)
    def str_suffix_rsa(self, value: str) -> object:
        from Crypto.PublicKey import RSA
        from Crypto.Cipher import PKCS1_OAEP
        return PKCS1_OAEP.new(RSA.import_key(value))
    def str_suffix_dsa(self, value: str) -> object:
        from Crypto.PublicKey import DSA
        from Crypto.Cipher import PKCS1_OAEP
        return PKCS1_OAEP.new(DSA.import_key(value))

class Arguments(object):
    def __init__(self, *args, **kwargs) -> None:
        self.args, self.kwargs = list(args), kwargs
    def __bool__(self) -> bool: return bool(self.args) or bool(self.kwargs)
    def __call__(self, callable_object: callable) -> object:
        return callable_object(*self.args, **self.kwargs)
    def __repr__(self) -> str:
        args_map = map(lambda arg: repr(arg), self.args)
        kwargs_map = map(lambda kwarg: '%s=%r' % kwarg, self.kwargs.items())
        return ', '.join([*args_map, *kwargs_map])
    def prepend(self, *args):
        self.args = [*args, *self.args]
        return self
    def append(self, *args):
        self.args = [*self.args, *args]
        return self
    def update(self, **kwargs):
        self.kwargs.update(**kwargs)
        return self
    def update_by_xmlattrs(self, xmlele: Element, **name2parsers: dict[str, callable]):
        for name, parser in name2parsers.items():
            if not xmlele.hasAttribute(name): continue
            self.kwargs[name] = parser(xmlele.getAttribute(name))
        return self

class DefaultDict(dict):
    def __init__(self, newfunc):
        super().__init__()
        self.newfunc = newfunc
    def __getitem__(self, key):
        if key not in self: super().__setitem__(key, self.newfunc(key))
        return super().__getitem__(key)

class SettingsMap(OrderedDict):
    def __init__(self, settings, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.settings = settings

    def install(self, from_object: object, to_object: object) -> object:
        self[from_object.name] = to_object
        setattr(self, from_object.name, to_object)
        return to_object
