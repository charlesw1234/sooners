from difflib import get_close_matches
from readline import set_completer
from typing import Iterable
from xml.dom.minidom import Element
from sqlalchemy import MetaData as SAMetaData
from ..utils import BaseHasher
from .operations import BaseOperation

class AnswerError(Exception): pass

class PatchCompleter(object):
    commands = ('create', 'unchanged', 'rename', 'drop')
    def __init__(self, names0: set[str], names1: set[str]) -> None:
        self.names0, self.names1 = names0, names1
        shortcuts = list()
        if bool(self.names0 & self.names1):
            shortcuts.append('unchanged %s' % ' '.join(self.names0 & self.names1))
        for name0 in names0 - names1:
            func = lambda name1: 'rename %s/%s' % (name0, name1)
            shortcuts.extend(map(func, get_close_matches(name0, names1)))
        func = lambda name1: 'create %s' % name1
        shortcuts.extend(map(func, sorted(names1 - names0)))
        func = lambda name0: 'drop %s' % name0
        shortcuts.extend(map(func, sorted(names0 - names1)))
        self.shortcuts = shortcuts[: 10]
        shortcut_commands = map(lambda idx: str(idx), range(len(self.shortcuts)))
        self.commands = (*self.__class__.commands, *shortcut_commands)

    def __call__(self, text: str, state: int) -> str:
        text_command, *text_names = text.split(' ')
        if not text_names: prefix, candidates, suffix = text_command, self.commands, ' '
        elif text_command == 'create':
            prefix, candidates, suffix = text_names[-1], self.names1, ' '
        elif text_command == 'unchanged':
            prefix, candidates, suffix = text_names[-1], self.names0 & self.names1, ' '
        elif text_command == 'rename' and '/' not in text_names[-1]:
            prefix, candidates, suffix = text_names[-1], self.names0, '/'
        elif text_command == 'rename' and '/' in text_names[-1]:
            prefix, candidates, suffix = text_names[-1].split('/')[-1], self.names1, ' '
        elif text_command == 'drop':
            prefix, candidates, suffix = text_names[-1], self.names0, ' '
        else: return None
        func0 = lambda candidate: candidate.startswith(prefix)
        func1 = lambda candidate: candidate[len(prefix) :]
        prompts = tuple(map(func1, filter(func0, candidates)))
        if state >= len(prompts): return None
        return '%s%s%s' % (text, prompts[state], suffix)

def xmlele_path(xmlele: Element) -> Iterable[Element]:
    while xmlele.nodeType is xmlele.ELEMENT_NODE:
        yield xmlele
        xmlele = xmlele.parentNode

class Doubt(Exception):
    def __init__(self, xmlpatch: Element,
                 names0: set[str], names1: set[str],
                 subtype: type) -> None:
        super().__init__()
        self.xmlpatch, self.subtype = xmlpatch, subtype
        self.names0, self.names1 = names0, names1
    def ask(self) -> None:
        completer = PatchCompleter(self.names0, self.names1)
        set_completer(completer)
        func = lambda xmlele: '%s(%s)' % (xmlele.nodeName, xmlele.getAttribute('name'))
        print('->'.join(map(func, xmlele_path(self.xmlpatch))))
        print('From(%s): %r' % (self.subtype.__name__, sorted(self.names0)))
        print('To  (%s): %r' % (self.subtype.__name__, sorted(self.names1)))
        for idx, shortcut in enumerate(completer.shortcuts):
            print('  Shortcut[%u]: %s' % (idx, shortcut))
        answer_text = input('Command: ').strip()
        names = set(self.names0)
        for answer in self.answer_from_text(answer_text, completer.shortcuts):
            if answer[0].lower() == 'create':
                for name in answer[1:]:
                    if name in names:
                        excfmt = 'Answer conflict detected: %r exists already.'
                        raise AnswerError(excfmt % name)
                    else: names.add(name)
                    self.append_one('Create', name = name)
            elif answer[0].lower() == 'unchanged':
                for name in answer[1:]:
                    if name not in names:
                        excfmt = 'Answer conflict detected: %r is not exists.'
                        raise AnswerError(excfmt % name)
                    self.append_one('', name = name)
            elif answer[0].lower() == 'rename':
                for namepair in answer[1:]:
                    name0, name1 = namepair.split('/')
                    if name0 not in names:
                        excfmt = 'Answer conflict detected: %r is not exists.'
                        raise AnswerError(excfmt % name0)
                    elif name1 in names:
                        excfmt = 'Answer conflict detected: %r exists already.'
                        raise AnswerError(excfmt % name1)
                    else: names.remove(name0); names.add(name1)
                    self.append_one('Rename', name0 = name0, name1 = name1)
            elif answer[0].lower() == 'drop':
                for name in answer[1:]:
                    if name not in names:
                        excfmt = 'Answer conflict detected: %r is not exists.'
                        raise AnswerError(excfmt % name)
                    else: names.remove(name)
                    self.append_one('Drop', name = name)
            else: raise AnswerError('Unsupported answer: %r.' % answer_text)
    def answer_from_text(self, answer_text: str, shortcuts: list[str]) -> Iterable[tuple[str]]:
        func0 = lambda part: not part.isdigit()
        func1 = lambda part: int(part) not in range(len(shortcuts))
        if any(map(func0, answer_text.split())): yield answer_text.split()
        elif any(map(func1, answer_text.split())):
            raise AnswerError('%r is not in %r.' %
                              tuple(filter(func1, answer_text.split())),
                              range(len(shortcuts)))
        else:
            for part in answer_text.split(): yield shortcuts[int(part)].split()
    def append_one(self, suffix: str, **attrs: dict[str, str]) -> None:
        nodename = '%s%s' % (self.subtype.__name__, suffix)
        subxmlpatch = self.xmlpatch.ownerDocument.createElement(nodename)
        for attr in attrs.items(): subxmlpatch.setAttribute(*attr)
        self.xmlpatch.appendChild(subxmlpatch)

class SA2SN(dict):
    sa2sn = dict()
    @classmethod
    def register(cls, *sooners_classes: tuple[type]) -> None:
        for sooners_class in sooners_classes:
            cls.sa2sn[sooners_class.__bases__[0]] = sooners_class
    @classmethod
    def cast(cls, obj):
        if obj.__class__ in cls.sa2sn: obj.__class__ = cls.sa2sn[obj.__class__]
        return obj

class SNBaseMixin(object):
    @classmethod
    def register_subtypes(cls, *subtypes: list[type]) -> None:
        cls.register_version_subtypes(*subtypes)
        cls.register_patch_subtypes(*subtypes)

class SNVersionMixin(SNBaseMixin):
    @classmethod
    def register_version_subtypes(cls, *subtypes: list[type]) -> None:
        func = lambda subtype: (subtype.__name__, subtype)
        cls.__sub_version_types__.update(map(func, subtypes))

    @classmethod
    def load_subobjs_from_xmlele(cls, xmlele: Element,
                                 metadata: SAMetaData) -> Iterable[SNBaseMixin]:
        func0 = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
        func1 = lambda subxmlele: subxmlele.nodeName in cls.__sub_version_types__.keys()
        for subxmlele in filter(func1, filter(func0, xmlele.childNodes)):
            subtype = cls.__sub_version_types__[subxmlele.nodeName]
            for subobj in subtype.new_from_xmlele(subxmlele, metadata):
                subobj.__xmlele__ = subxmlele
                yield subobj

    @classmethod
    def save_to_xmlele(cls, xmlele: Element, objgroup: tuple[SNBaseMixin],
                       hasher: BaseHasher, **kwargs) -> Element:
        assert(xmlele.nodeName == cls.__name__)
        assert(all(map(lambda obj: isinstance(obj, cls), objgroup)))
        hasher.update(cls.save_to_xmlele_open(xmlele, objgroup, **kwargs))
        subobjs = objgroup[0].generate_subobjs(**kwargs)
        for subobjgroup in objgroup[0].generate_subobj_groups(*subobjs):
            subcls = subobjgroup[0].__class__
            subxmlele = xmlele.ownerDocument.createElement(subcls.__name__)
            xmlele.appendChild(subxmlele)
            subcls.save_to_xmlele(subxmlele, subobjgroup, hasher, **kwargs)
        hasher.update(cls.save_to_xmlele_close(xmlele, objgroup, **kwargs))
        return xmlele
    @classmethod
    def save_to_xmlele_close(cls, xmlele: Element,
                             objgroup: tuple[SNBaseMixin], **kwargs) -> str:
        return ''

    def generate_subobj_groups(
            self, *subobjs: list[SNBaseMixin]) -> Iterable[list[SNBaseMixin]]:
        if not hasattr(self, '__sub_version_types__'): subtypes = ()
        else: subtypes = tuple(self.__sub_version_types__.values())
        func0 = lambda subobj: SA2SN.cast(subobj)
        func1 = lambda subobj: isinstance(subobj, subtypes)
        done, subobjs = set(), tuple(filter(func1, map(func0, subobjs)))
        for idx, subobj in enumerate(subobjs):
            if subobj in done: continue
            func0 = lambda subobj0: subobj0 not in done
            func1 = lambda subobj0: subobj.__class__ is subobj0.__class__
            func2 = lambda subobj0: subobj.group_check(subobj0)
            subobjs0 = tuple(filter(func2, filter(func1, filter(func0, subobjs[idx:]))))
            done.update(subobjs0)
            yield subobjs0
    def generate_subobjs(self, **kwargs) -> tuple[SNBaseMixin]: return ()
    def group_check(self, other) -> bool: return self.name == other.name

class SNPatchMixin(SNBaseMixin):
    @classmethod
    def register_patch_subtypes(cls, *subtypes: list[type]) -> None:
        func = lambda subtype: ('%sCreate' % subtype.__name__, subtype)
        cls.__sub_patch_create_types__.update(map(func, subtypes))
        func = lambda subtype: ('%s' % subtype.__name__, subtype)
        cls.__sub_patch_types__.update(map(func, subtypes))
        func = lambda subtype: ('%sRename' % subtype.__name__, subtype)
        cls.__sub_patch_rename_types__.update(map(func, subtypes))
        func = lambda subtype: ('%sDrop' % subtype.__name__, subtype)
        cls.__sub_patch_drop_types__.update(map(func, subtypes))

    @classmethod
    def raise_doubt(cls, xmlpatch: Element,
                    xmlversion0: Element, xmlversion1: Element) -> Element:
        if not hasattr(cls, '__sub_version_types__'): return
        for subtype in cls.__sub_version_types__.values():
            group0 = cls.patch_group_by_subtype(xmlversion0, subtype)
            group1 = cls.patch_group_by_subtype(xmlversion1, subtype)
            names0, names1 = set(group0.keys()), set(group1.keys())
            nodename_create = '%sCreate' % subtype.__name__
            nodename_unchanged = subtype.__name__
            nodename_rename = '%sRename' % subtype.__name__
            nodename_drop = '%sDrop' % subtype.__name__
            func = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
            for subxmlpatch in filter(func, xmlpatch.childNodes):
                if subxmlpatch.nodeName == nodename_create:
                    names1.remove(subxmlpatch.getAttribute('name'))
                elif subxmlpatch.nodeName == nodename_unchanged:
                    name = subxmlpatch.getAttribute('name')
                    names0.remove(name); names1.remove(name)
                elif subxmlpatch.nodeName == nodename_rename:
                    names0.remove(subxmlpatch.getAttribute('name0'))
                    names1.remove(subxmlpatch.getAttribute('name1'))
                elif subxmlpatch.nodeName == nodename_drop:
                    names0.remove(subxmlpatch.getAttribute('name'))
                else: continue
            if not bool(names0):
                for name1 in names1:
                    cls.patch_append_one(xmlpatch, nodename_create, name = name1)
            elif not bool(names1):
                for name0 in names0:
                    cls.patch_append_one(xmlpatch, nodename_drop, name = name0)
            elif names0 != names1: raise Doubt(xmlpatch, names0, names1, subtype)
            else:
                for name in names0:
                    cls.patch_append_one(xmlpatch, nodename_unchanged, name = name)
            for subxmlpatch in filter(func, xmlpatch.childNodes):
                if subxmlpatch.nodeName == nodename_create: continue
                elif subxmlpatch.nodeName == nodename_unchanged:
                    name = subxmlpatch.getAttribute('name')
                    subtype.raise_doubt(subxmlpatch, group0[name], group1[name])
                elif subxmlpatch.nodeName == nodename_rename:
                    name0 = subxmlpatch.getAttribute('name0')
                    name1 = subxmlpatch.getAttribute('name1')
                    subtype.raise_doubt(subxmlpatch, group0[name0], group1[name1])
                elif subxmlpatch.nodeName == nodename_drop: continue
                else: continue
        return xmlpatch
    @classmethod
    def patch_group_by_subtype(cls, xmlversion: Element, subtype: type) -> dict[str, Element]:
        func0 = lambda subnode: subnode.nodeType is subnode.ELEMENT_NODE
        func1 = lambda subxmlele: subxmlele.nodeName == subtype.__name__
        func2 = lambda subxmlele: (subxmlele.getAttribute('name'), subxmlele)
        return dict(map(func2, filter(func1, filter(func0, xmlversion.childNodes))))
    @classmethod
    def patch_append_one(cls, xmlpatch: Element,
                         nodename: str, **attrs: dict[str, str]) -> None:
        subxmlpatch = xmlpatch.ownerDocument.createElement(nodename)
        for attr in attrs.items(): subxmlpatch.setAttribute(*attr)
        xmlpatch.appendChild(subxmlpatch)

    @classmethod
    def do_params_update(cls, xmlversion: Element,
                         version0: SNBaseMixin, version1: SNBaseMixin,
                         **kwargs) -> Iterable[BaseOperation]:
        assert(version0.__class__ is version1.__class__)
        func = lambda xmlnode: xmlnode.nodeType is xmlnode.ELEMENT_NODE
        for subxmlversion in filter(func, xmlversion.childNodes):
            if subxmlversion.nodeName in cls.__sub_version_types__:
                subcls = cls.__sub_version_types__[subxmlversion.nodeName]
                if not hasattr(subcls, 'params_update'): continue
                for operation in subcls.params_update(
                        subxmlversion, version0, version1, **kwargs):
                    yield operation
            else: continue

    @classmethod
    def do_forward(cls, xmlpatch: Element,
                   version0: SNBaseMixin, version1: SNBaseMixin,
                   **kwargs) -> Iterable[BaseOperation]:
        assert(version0.__class__ is version1.__class__)
        func = lambda xmlnode: xmlnode.nodeType is xmlnode.ELEMENT_NODE
        for subxmlpatch in filter(func, xmlpatch.childNodes):
            if subxmlpatch.nodeName in cls.__sub_patch_create_types__:
                subcls = cls.__sub_patch_create_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_forward_create'): continue
                for operation in subcls.patch_forward_create(subxmlpatch, version1, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_types__:
                subcls = cls.__sub_patch_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_forward'): continue
                for operation in subcls.patch_forward(
                        subxmlpatch, version0, version1, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_rename_types__:
                subcls = cls.__sub_patch_rename_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_forward_rename'): continue
                for operation in subcls.patch_forward_rename(
                        subxmlpatch, version0, version1, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_drop_types__:
                subcls = cls.__sub_patch_drop_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_forward_drop'): continue
                for operation in subcls.patch_forward_drop(subxmlpatch, version0, **kwargs):
                    yield operation
            else: continue
    @classmethod
    def do_backward(cls, xmlpatch: Element,
                    version1: SNBaseMixin, version0: SNBaseMixin,
                    **kwargs) -> Iterable[BaseOperation]:
        assert(version0.__class__ is version1.__class__)
        func = lambda xmlnode: xmlnode.nodeType is xmlnode.ELEMENT_NODE
        for subxmlpatch in filter(func, reversed(xmlpatch.childNodes)):
            if subxmlpatch.nodeName in cls.__sub_patch_create_types__:
                subcls = cls.__sub_patch_create_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_backward_create'): continue
                for operation in subcls.patch_backward_create(subxmlpatch, version1, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_types__:
                subcls = cls.__sub_patch_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_backward'): continue
                for operation in subcls.patch_backward(
                        subxmlpatch, version1, version0, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_rename_types__:
                subcls = cls.__sub_patch_rename_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_backward_rename'): continue
                for operation in subcls.patch_backward_rename(
                        subxmlpatch, version1, version0, **kwargs):
                    yield operation
            elif subxmlpatch.nodeName in cls.__sub_patch_drop_types__:
                subcls = cls.__sub_patch_drop_types__[subxmlpatch.nodeName]
                if not hasattr(subcls, 'patch_backward_drop'): continue
                for operation in subcls.patch_backward_drop(subxmlpatch, version0, **kwargs):
                    yield operation
            else: continue
