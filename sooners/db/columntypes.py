from base64 import urlsafe_b64encode, urlsafe_b64decode
from datetime import date, datetime, time, timedelta
from enum import Enum as PyEnum
from xml.dom.minidom import Element
from typing import Iterable
from sqlalchemy import BigInteger as SABigInteger
from sqlalchemy import Boolean as SABoolean
from sqlalchemy import Date as SADate
from sqlalchemy import DateTime as SADateTime
from sqlalchemy import Enum as SAEnum
from sqlalchemy import Float as SAFloat
from sqlalchemy import Integer as SAInteger
from sqlalchemy import Interval as SAInterval
from sqlalchemy import LargeBinary as SALargeBinary
from sqlalchemy import Numeric as SANumeric
from sqlalchemy import SmallInteger as SASmallInteger
from sqlalchemy import String as SAString
from sqlalchemy import Text as SAText
from sqlalchemy import Time as SATime
from sqlalchemy import Unicode as SAUnicode
from sqlalchemy import UnicodeText as SAUnicodeText
from ..utils import Arguments
from .mixins import SA2SN

def bool_parser(text: str) -> bool:
    if text == 'True': return True
    elif text == 'False': return False
    else: raise ValueError('Invalid text of boolean: %r.' % text)

class ColumnTypeMixin(object):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments

    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        yield ('type', self.__class__.__name__)
    def save_to_subeles(self, xmlele: Element) -> None: pass
    def format(self, value: object) -> str: return repr(value)
    def parse(self, text: str) -> object: raise NotImplemented
    def equal(self, other) -> bool: return type(self) is type(other)

class ColumnStringMixin(ColumnTypeMixin):
    def format(self, value: str) -> str: return value
    def parse(self, text: str) -> str: return text

class ColumnIntMixin(ColumnTypeMixin):
    def format(self, value: int) -> str: return str(value)
    def parse(self, text: str) -> int: return int(text)

class BigInteger(SABigInteger, ColumnIntMixin): pass

class Boolean(SABoolean, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, create_constraint = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        if self.create_constraint: yield ('create_constraint', repr(self.create_constraint))
    def parse(self, text: str) -> bool: return bool_parser(text)
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.create_constraint == other.create_constraint

class Date(SADate, ColumnTypeMixin):
    date_format = '%Y%m%d'
    def format(self, value: date) -> str: return value.strftime(self.date_format)
    def parse(self, text: str) -> date: return datetime.strptime(text, self.date_format).date()

class DateTime(SADateTime, ColumnTypeMixin):
    datetime_format = '%Y%m%d-%H%M%S.%f'
    datetime_tz_format = '%Y%m%d-%H%M%S.%f.%Z'
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, timezone = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        if self.timezone: yield ('timezone', repr(self.timezone))
    def format(self, value: datetime) -> str:
        if value.tzinfo is None: return value.strftime(self.datetime_format)
        else: return value.strftime(self.datetime_timezone_format)
    def parse(self, text: str) -> datetime:
        ndots = len(text.split('.')) - 1
        if ndots == 1: return datetime.strptime(text, self.datetime_format)
        elif ndots == 2: return datetime.strptime(text, self.datetime_tz_format)
        raise ValueError('Invalid text for DateTime: %r.' % text)
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.timezone == other.timezone

class Enum(SAEnum, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        func0 = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
        func1 = lambda subxmlele: subxmlele.nodeName == 'EnumValue'
        func2 = lambda subxmlele: (
            subxmlele.getAttribute('name'), eval(subxmlele.getAttribute('value')))
        enum_values = tuple(map(func2, filter(func1, filter(func0, xmlele.childNodes))))
        return arguments.append(PyEnum(xmlele.getAttribute('enum_name'), enum_values))
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('enum_name', self.name)
    def save_to_subeles(self, xmlele: Element) -> None:
        func = lambda enum_value: enum_value.value
        for enum_value in sorted(self.enum_class.__members__.values(), key = func):
            subxmlele = xmlele.ownerDocument.createElement('EnumValue')
            subxmlele.setAttribute('name', enum_value.name)
            subxmlele.setAttribute('value', repr(enum_value.value))
            xmlele.appendChild(subxmlele)
    def format(self, value: PyEnum) -> str: return value.name
    def parse(self, text: str) -> PyEnum: return self.enum_class.__members__[text]
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        if self.enum_class.__name__ != other.enum_class.__name__: return False
        names0 = set(self.enum_class.__members__.keys())
        names1 = set(other.enum_class.__members__.keys())
        if names0 != names1: return False
        func = lambda name: self.enum_class[name] == other.enum_class[name]
        return all(map(func, names0))
    def post_operation(self, prompt: callable, database_name: str, oper) -> None:
        dialect = oper.get_bind().dialect
        # dialect.supports_native_enum can not be used yet.
        from alembic.ddl.postgresql import PostgresqlImpl
        if isinstance(oper.impl, PostgresqlImpl):
            from sqlalchemy.dialects import postgresql
            prompt('%s@%s(%r)' % ('drop_enum_type', database_name, self.enum_class.__name__))
            preparer = dialect.preparer(dialect)
            enum = postgresql.ENUM(name = preparer.format_type(self))
            oper.execute(postgresql.DropEnumType(enum))

class Float(SAFloat, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, asdecimal = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        if self.asdecimal: yield ('asdecimal', repr(self.asdecimal))
    def parse(self, text: str) -> float: return float(text)
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.asdecimal == other.asdecimal

class Integer(SAInteger, ColumnIntMixin): pass

class Interval(SAInterval, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(
            xmlele, native = bool_parser, second_precision = int, day_precision = int)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        if self.native: yield ('native', repr(self.native))
        yield ('second_precision', int(self.second_precision))
        yield ('day_precision', int(self.day_precision))
    def format(self, value: timedelta) -> str:
        hour = value.seconds // 3600
        minute = value.seconds // 60 % 60
        second = value.seconds % 60
        return '%u.%02u:%02u:%02u.%06u' % (
            value.days, hour, minute, second, value.microseconds)
    def parse(self, text: str) -> timedelta:
        days, time, microseconds = text.split('.')
        hour, minute, second = time.split(':')
        return timedelta(days = int(days), hour = int(hour), minute = int(minute),
                         second = int(second), microseconds = int(microseconds))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return (self.native == other.native and
                self.second_precision == other.second_precision and
                self.day_precision == other.day_precision)

class LargeBinary(SALargeBinary, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, length = int)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('length', repr(self.length))
    def format(self, value: bytes) -> str: return urlsafe_b64encode(value)
    def parse(cls, text: str) -> bytes: return urlsafe_b64decode(text)
    def equal(self, other) -> str:
        if not super().equal(other): return False
        return self.length == other.length

class Numeric(SANumeric, ColumnTypeMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(
            xmlele, precision = int, scale = int, asdecimal = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('precision', repr(self.precision))
        yield ('scale', repr(self.scale))
        if not self.asdecimal: yield ('asdecimal', repr(self.asdecimal))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return (self.precision == other.precision and
                self.scale == other.scale and
                self.asdecimal == other.asdecimal)

class SmallInteger(SASmallInteger, ColumnIntMixin): pass

class String(SAString, ColumnStringMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, length = int, collation = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('length', repr(self.length))
        if self.collation: yield ('collation', repr(self.collation))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.length == other.length and self.collation == other.collation

class Text(SAText, ColumnStringMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, length = int, collation = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('length', repr(self.length))
        if self.collation: yield ('collation', repr(self.collation))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.length == other.length and self.collation == other.collation

class Time(SATime, ColumnTypeMixin):
    time_format = '%H%M%S.%f'
    time_tz_format = '%H%M%S.%f.%Z'
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, timezone = bool_parser)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        if self.timezone: yield ('timezone', repr(self.timezone))
    def format(self, value: time) -> str:
        if value.tzinfo is None: return value.strftime(self.time_format)
        else: return value.strftime(self.time_tz_format)
    def parse(self, text: str) -> time:
        ndots = len(text.split('.')) - 1
        if ndots == 1: return datetime.strptime(text, self.time_format).time()
        elif ndots == 2: return datetime.strptime(text, self.time_tz_format).time()
        raise ValueError('Invalid text for Time: %r.' % text)
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.timezone == other.timezone

class Unicode(SAUnicode, ColumnStringMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, length = int)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('length', repr(self.length))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.length == other.length

class UnicodeText(SAUnicodeText, ColumnStringMixin):
    @classmethod
    def arguments_from_xmlele(cls, arguments: Arguments, xmlele: Element) -> Arguments:
        return arguments.update_by_xmlattrs(xmlele, length = int)
    def save_to_attrs(self) -> Iterable[tuple[str, str]]:
        for attr in super().save_to_attrs(): yield attr
        yield ('length', repr(self.length))
    def equal(self, other) -> bool:
        if not super().equal(other): return False
        return self.length == other.length

class ColumnTypeMap(dict):
    def register_column_types(self, *column_types):
        SA2SN.register(*column_types)
        func = lambda column_type: (column_type.__name__, column_type)
        self.update(map(func, column_types))
    def new_from_xmlele(self, xmlele: Element) -> ColumnTypeMixin:
        column_type_class = self[xmlele.getAttribute('type')]
        arguments = column_type_class.arguments_from_xmlele(Arguments(), xmlele)
        return arguments(column_type_class)
column_type_map = ColumnTypeMap()
column_type_map.register_column_types(
    BigInteger, Boolean, Date, DateTime, Enum, Float, Integer, LargeBinary,
    Numeric, SmallInteger, String, Text, Time, Unicode, UnicodeText)
