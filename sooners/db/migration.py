from typing import Iterable
from ..utils import Context, DefaultDict, OrderedDict
from ..component import BaseComponent
from .operations import BaseOperation
from .metadata import MetaDataSaved
from ..core.models import Configuration, DBSchemaVersion, DBSchemaOperation

class DBSchemaParams(object):
    def __init__(self, context: Context) -> None:
        self.params0_text = Configuration.load_configuration(
            Configuration.CONF_TYPE.SCHEMA_PARAMS_0, context)
        self.params1_text = Configuration.load_configuration(
            Configuration.CONF_TYPE.SCHEMA_PARAMS_1, context)
    def __repr__(self) -> str:
        text0, text1 = repr(self.params0_text), repr(self.params1_text)
        return '%s(%s,%s)' % (self.__class__.__name__, text0, text1)
    # fixme: it is dangerous to use repr/eval, may be reimplement in another way.
    def params0(self) -> Context | None:
        return None if self.params0_text is None else\
            Context(deep = True, **eval(self.params0_text))
    def save_params0(self, params0: Context | None = None) -> None:
        if params0 is None: self.params0_text = None
        else: self.params0_text = repr(params0.to_dict_deep())
    def params1(self) -> Context | None:
        return None if self.params1_text is None else\
            Context(deep = True, **eval(self.params1_text))
    def save_params1(self, params1: Context | None = None) -> None:
        if params1 is None: self.params1_text = None
        else: self.params1_text = repr(params1.to_dict_deep())
    def save_configuration(self, context: Context, commit_ornot: bool = True) -> bool:
        saved0_ornot = Configuration.save_configuration(
            Configuration.CONF_TYPE.SCHEMA_PARAMS_0, self.params0_text,
            context, commit_ornot = False)
        saved1_ornot = Configuration.save_configuration(
            Configuration.CONF_TYPE.SCHEMA_PARAMS_1, self.params1_text,
            context, commit_ornot = False)
        if commit_ornot: context.default.session.commit()
        return saved0_ornot and saved1_ornot

class _AutoOperations(object):
    def __init__(self, component_name: str, context: Context) -> None:
        self.component_name, self.context = component_name, context
    def __call__(self, database_name: str) -> dict[str, DBSchemaOperation]:
        (inspector := self.context.inspectors[database_name]).clear_cache()
        if not inspector.has_table(DBSchemaOperation.__tablename__): return dict()
        query = self.context.sessions[database_name].query(DBSchemaOperation)
        func = lambda record: (record.key(), record)
        return dict(map(func, query.filter_by(component_name = self.component_name)))

class Migration(object):
    def __init__(self, context: Context) -> None:
        self.params_record = DBSchemaParams(context)
        self.version_records = DBSchemaVersion.load_default_dict(context)
        self.metadata0 = self.load_metadata0(context.settings)
        if self.is_clean(): self.metadata1 = self.metadata0
        else: self.metadata1 = self.load_metadata1(context.settings)
        self.load_xmlpatches(context.settings)

    def __repr__(self) -> str:
        return '%s(%r,%r)' % (self.__class__.__name__, self.metadata0, self.metadata1)

    def is_clean(self) -> bool:
        if self.params_record.params0_text != self.params_record.params1_text: return False
        for version_record in self.version_records.values():
            if version_record.version0 != version_record.version1: return False
            if version_record.checksum0 != version_record.checksum1: return False
        return True

    def load_metadata0(self, settings) -> MetaDataSaved | None:
        key_func = lambda vrecord: vrecord.index0
        version_records = sorted(self.version_records.values(), key = key_func)
        func0 = lambda vrecord: vrecord.version0 is not None
        func1 = lambda vrecord: (vrecord.component_name, vrecord.xmlversion0(settings))
        xmlversions0 = dict(map(func1, filter(func0, version_records)))
        if not xmlversions0: return None
        return MetaDataSaved(settings, xmlversions0, self.params_record.params0())
    def load_metadata1(self, settings) -> MetaDataSaved | None:
        key_func = lambda vrecord: vrecord.index1
        version_records = sorted(self.version_records.values(), key = key_func)
        func0 = lambda vrecord: vrecord.version1 is not None
        func1 = lambda vrecord: (vrecord.component_name, vrecord.xmlversion1(settings))
        xmlversions1 = dict(map(func1, filter(func0, version_records)))
        if not xmlversions1: return None
        return MetaDataSaved(settings, xmlversions1, self.params_record.params1())
    def load_xmlpatches(self, settings) -> None:
        func0 = lambda vrecord: vrecord.need_patch()
        func1 = lambda vrecord: (vrecord.component_name, vrecord.xmlpatch(settings))
        self.xmlpatches = dict(map(func1, filter(func0, self.version_records.values())))

    def same_metadata0(self, metadata0: MetaDataSaved) -> bool:
        if self.params_record.params0() != metadata0.params: return False
        for component in metadata.components.values():
            if not self.version_records[component.name].same0(component): return False
        for version_record in self.version_records.values():
            if version_record.component_name in metadata0.components: continue
            if version_record.version0 is not None: return False
            if version_record.checksum0 is not None: return False
        return True
    def same_metadata1(self, metadata1: MetaDataSaved) -> bool:
        if self.params_record.params1() != metadata1.params: return False
        for component in metadata.components.values():
            if not self.version_records[component.name].same1(component): return False
        for version_record in self.version_records.values():
            if version_record.component_name in metadata1.component: continue
            if version_record.version1 is not None: return False
            if version_record.checksum1 is not None: return False
        return True

    def set_metadata0_none(self, context: Context) -> None:
        self.params_record.save_params0(None)
        for version_record in self.version_records.values(): version_record.save0_none()
        self.metadata0 = None
        self._smart_save(context)
    def set_metadata1_none(self, context: Context) -> None:
        self.params_record.save_params1(None)
        for version_record in self.version_records.values(): version_record.save1_none()
        self.metadata1 = None
        self._smart_save(context)

    def set_metadata0(self, context: Context, metadata0: MetaDataSaved) -> None:
        self.params_record.save_params0(metadata0.params)
        for index, component_context in enumerate(metadata0.components.values()):
            self.version_records[component_context.name].save0(index, component_context)
        for version_record in self.version_records.values():
            if version_record.component_name in metadata0.components: continue
            version_record.save0_none()
        self.metdata0 = metadata0
        self._smart_save(context)
        self.load_xmlpatches(context.settings)
    def set_metadata1(self, context: Context, metadata1: MetaDataSaved) -> None:
        self.params_record.save_params1(metadata1.params)
        for index, component_context in enumerate(metadata1.components.values()):
            self.version_records[component_context.name].save1(index, component_context)
        for version_record in self.version_records.values():
            if version_record.component_name in metadata1.components: continue
            version_record.save1_none()
        self.metadata1 = metadata1
        self._smart_save(context)
        self.load_xmlpatches(context.settings)

    def direction(self) -> tuple[bool, bool]:
        func0 = lambda vrecord: None not in (vrecord.version0, vrecord.version1)
        func1 = lambda vrecord: vrecord.version0 < vrecord.version1
        forward_patch = any(map(func1, filter(func0, self.version_records.values())))
        func1 = lambda vrecord: vrecord.version0 > vrecord.version1
        backward_patch = any(map(func1, filter(func0, self.version_records.values())))
        func = lambda vrecord: vrecord.version0 is None and vrecord.version1 is not None
        forward_create = any(map(func, self.version_records.values()))
        func = lambda vrecord: vrecord.version0 is not None and vrecord.version1 is None
        backward_drop = any(map(func, self.version_records.values()))
        return forward_patch or forward_create, backward_patch or backward_drop

    def do_operations_forward(self, context: Context,
                              database_names: set[str] = None) -> Iterable[BaseOperation]:
        for component_name in self.metadata1.components.keys():
            component = context.settings.components[component_name]
            for delayed_operation in self._do_operations_by_component(
                    context, component, database_names):
                yield delayed_operation
        for component_name in self.metadata1.components.keys():
            component = context.settings.components[component_name]
            self._clean_operation_records(context, component, database_names)
        self._finish(context)

    def do_operations_backward(self, context: Context,
                               database_names: set[str] = None) -> Iterable[BaseOperation]:
        delayed_operations = list()
        for component_name in reversed(self.metadata0.components.keys()):
            component = context.settings.components[component_name]
            for delayed_operation in self._do_operations_by_component(
                    context, component, database_names):
                yield delayed_operation
        for component_name in reversed(self.metadata0.components.keys()):
            component = context.settings.components[component_name]
            self._clean_operation_records(context, component, database_names)
        self._finish(context)

    def _do_operations_by_component(
            self, context: Context, component: BaseComponent,
            database_names: set[str] = None) -> Iterable[BaseOperation]:
        # yield the delayed operations to the caller.
        operations = self._generate_operations(component)
        if database_names is None: func = lambda operation: True
        else: func = lambda operation: operation.database_name in database_names
        dbname2key2record = DefaultDict(_AutoOperations(component.name, context))
        for operation in filter(func, operations):
            if operation.key() in dbname2key2record[operation.database_name]: continue
            elif self._is_delay_operation(operation): yield operation
            else: self._do_operation(context, operation, component, dbname2key2record)

    def _generate_operations(self, component: BaseComponent) -> tuple[BaseOperation]:
        if self.metadata0 is None: return self.metadata1.do_create(component)
        elif self.metadata1 is None: return self.metadata0.do_drop(component)
        version0 = self.metadata0.components[component.name].version
        version1 = self.metadata1.components[component.name].version
        if version0 < version1:
            return MetaDataSaved.do_forward(self.xmlpatches[component.name],
                                            self.metadata0, self.metadata1)
        elif version0 > version1:
            return MetaDataSaved.do_backward(self.xmlpatches[component.name],
                                             self.metadata0, self.metadata1)
        elif self.params_record.params0 != self.params_record.params1:
            assert(self.metadata0.components[component.name].xmlversion is
                   self.metadata1.components[component.name].xmlversion)
            return MetaDataSaved.do_params_update(
                self.metadata0.components[component.name].xmlversion,
                self.metadata0, self.metadata1)
        return ()

    def _is_delay_operation(self, operation: BaseOperation) -> bool:
        if operation.oper_member != 'drop_table': return False
        return operation.table.name in (
            Configuration.__tablename__, DBSchemaVersion.__tablename__,
            DBSchemaOperation.__tablename__)

    def _do_operation(self, context: Context, operation: BaseOperation,
                      component: BaseComponent, dbname2key2record: DefaultDict) -> None:
        self._before_operation(context, operation, component, dbname2key2record)
        operation(context.prompt, context.operators[operation.database_name])
        self._after_operation(context, operation, component, dbname2key2record)

    def _before_operation(self, context: Context, operation: BaseOperation,
                          component: BaseComponent, dbname2key2record: DefaultDict) -> None:
        pass

    def _after_operation(self, context: Context, operation: BaseOperation,
                         component: BaseComponent, dbname2key2record: DefaultDict) -> None:
        record = DBSchemaOperation.new_by_operation(component.name, operation)
        dbname2key2record[operation.database_name][operation.key()] = record
        (inspector := context.inspectors[operation.database_name]).clear_cache()
        if inspector.has_table(DBSchemaOperation.__tablename__):
            session = context.sessions[operation.database_name]
            session.add(record)
            session.commit()
        if operation.oper_member == 'create_table':
            if operation.table.name == Configuration.__tablename__:
                self.params_record.save_configuration(context)
            elif operation.table.name == DBSchemaVersion.__tablename__:
                DBSchemaVersion.save_default_dict(context, self.version_records)

    def _clean_operation_records(self, context: Context, component: BaseComponent,
                                 database_names: set[str] = None) -> None:
        if database_names is None: func = lambda database_name: True
        else: func = lambda database_name: database_name in database_names
        for database_name, inspector in filter(func, context.inspectors.items()):
            inspector.clear_cache()
            if not inspector.has_table(DBSchemaOperation.__tablename__): continue
            query = context.sessions[database_name].query(DBSchemaOperation)
            query.filter_by(component_name = component.name).delete()
            context.sessions[database_name].commit()

    def _finish(self, context: Context) -> None:
        self.params_record.params0_text = self.params_record.params1_text
        for version_record in self.version_records.values():
            version_record.index0 = version_record.index1
            version_record.version0 = version_record.version1
            version_record.checksum0 = version_record.checksum1
        self._smart_save(context)
        self.metadata0, self.xmlpatches = self.metadata1, dict()

    def _smart_save(self, context: Context) -> None:
        self.params_record.save_configuration(context, commit_ornot = False)
        DBSchemaVersion.save_default_dict(context, self.version_records, commit_ornot = False)
        context.default.session.commit()
