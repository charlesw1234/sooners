from typing import Iterable
from xml.dom.minidom import getDOMImplementation, Element
from sqlalchemy import MetaData as SAMetaData
from ..utils import Hasher, Context
from ..component import BaseComponent
from .database import BaseDatabase
from .mixins import Doubt, SNBaseMixin, SNVersionMixin, SNPatchMixin
from .operations import BaseOperation, CreateTable, DropTable
from .table import Table, BatchTable

class BaseMetaData(SAMetaData, SNVersionMixin, SNPatchMixin):
    __sub_version_types__ = dict()
    __sub_patch_create_types__, __sub_patch_types__ = dict(), dict()
    __sub_patch_rename_types__, __sub_patch_drop_types__ = dict(), dict()
    def __init__(self, settings, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.settings = settings
BaseMetaData.register_subtypes(Table, BatchTable)

def make_patch(xmlversion0: Element, xmlversion1: Element, prompt: callable) -> Element:
    xmlpatch = getDOMImplementation().createDocument(None, 'Patch', None).documentElement
    while True:
        try: BaseMetaData.raise_doubt(xmlpatch, xmlversion0, xmlversion1)
        except Doubt as doubt:
            try: doubt.ask()
            except AnswerError as exc: prompt(repr(exc), opts = ('bold',))
        else: break
    return xmlpatch

class MetaDataSaved(BaseMetaData):
    @classmethod
    def versions2xmls(cls, settings, versions: Context) -> Context:
        func0 = lambda n2v: n2v[0] in settings.components
        func1 = lambda n2v: (n2v[0], settings.components[n2v[0]].version_parse(n2v[1]))
        return dict(map(func1, filter(func0, versions.items())))
    def __init__(self, settings, xmlversions: Context,
                 params: Context, *args, **kwargs) -> None:
        super().__init__(settings, *args, **kwargs)
        self.params, self.components = params, dict()
        for name, xmlversion in xmlversions.items():
            assert(name == xmlversion.getAttribute('component'))
            context = Context(name = name, xmlversion = xmlversion,
                              version = int(xmlversion.getAttribute('version')),
                              checksum = xmlversion.getAttribute('checksum'))
            self.components[context.name] = context
            component = settings.components[context.name]
            for table in self.load_subobjs_from_xmlele(xmlversion, self):
                table.__component__ = component
    def __repr__(self) -> str:
        func = lambda context: '%s=%u' % (context.name, context.version)
        content = ', '.join(map(func, self.components.values()))
        return '%s(%s)' % (self.__class__.__name__, content)
    def do_create(self, component = None) -> Iterable[BaseOperation]:
        if component is None: func = lambda table: True
        else: func = lambda table: table.__component__ is component
        for table in filter(func, self.sorted_tables):
            for database_name in sorted(table.__database_names__):
                yield CreateTable(database_name, table)
    def do_drop(self, component = None) -> Iterable[BaseOperation]:
        if component is None: func = lambda table: True
        else: func = lambda table: table.__component__ is component
        for table in filter(func, reversed(self.sorted_tables)):
            for database_name in sorted(table.__database_names__, reverse = True):
                yield DropTable(database_name, table)

class ModelCatalogue(object):
    def __init__(self) -> None:
        self.plain_abstract, self.plain = dict(), dict()
        self.batch_abstract, self.batch, self.batch_entity = dict(), dict(), dict()
    def __repr__(self) -> str:
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            map(lambda name: '%s: %r' % (name, tuple(getattr(self, name).keys())),
                ('plain_abstract', 'plain', 'batch_abstract', 'batch', 'batch_entity'))))
    def traverse(self) -> Iterable:
        for model_dict in (self.plain_abstract, self.batch_abstract,
                           self.plain, self.batch, self.batch_entity):
            for name, model in model_dict.items():
                yield name, model
    def setup(self, params: Context, databases: dict[str, BaseDatabase]) -> None:
        for name, model in self.traverse():
            model.setup(params, databases)
            if not hasattr(model, '__database_names__'): continue
            if not hasattr(model, '__table__'): continue
            model.__table__.__database_names__ = model.__database_names__

class MetaData(BaseMetaData):
    @classmethod
    def save_to_xmlele_open(cls, xmlele: Element,
                            objgroup: tuple[SNBaseMixin],
                            component = None, **kwargs) -> None:
        assert(len(objgroup) == 1)

    def __init__(self, settings, *args, **kwargs):
        super().__init__(settings, *args, **kwargs)
        self.models = ModelCatalogue()
    def __repr__(self): return '%s(%s)' % (self.__class__.__name__, self.settings.__module__)

    def make_version(self, component: BaseComponent) -> Element | None:
        xmlele = getDOMImplementation().createDocument(None, 'MetaData', None).documentElement
        self.save_to_xmlele(xmlele, (self,), component = component)
        func = lambda subxmlnode: subxmlnode.nodeType is subxmlnode.ELEMENT_NODE
        if not any(map(func, xmlele.childNodes)): return None
        xmlele.setAttribute('checksum', Hasher(xmlele.toxml()).b64digest())
        return xmlele

    def save_params(self, params: Context | None = None) -> Context:
        if params is None: params = Context()
        for subobj_group in self.generate_subobj_groups(*self.sorted_tables):
            params = subobj_group[0].__class__.save_params(params, subobj_group)
        return params

    def generate_subobjs(self, component: BaseComponent | None = None,
                         **kwargs) -> tuple[SNBaseMixin]:
        if component is None: func0 = lambda table: True
        else: func0 = lambda table: table.__component__ is component
        func1 = lambda table: getattr(table, '__batch_suffix__', None) is None
        return tuple(filter(func1, filter(func0, self.sorted_tables)))
