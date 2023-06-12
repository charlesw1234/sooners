from typing import Iterable
from . import BaseMilestoneStep
from ..utils import Context
from ..db.operations import BaseOperation
from ..db.metadata import MetaDataSaved
from ..db.migration import Migration

class DBSchemaStep(BaseMilestoneStep):
    def __init__(self, versions: dict[str, int], params: dict[str, object]):
        super().__init__()
        self.versions, self.params = versions, params

    def description(self):
        func = lambda item: '%s=%r' % item
        return '(%s)-(%s)' % (','.join(map(func, self.versions.items())),
                              ','.join(map(func, self.params.items())))

    def setup_metadata1(self) -> MetaDataSaved:
        from ..settings import the_settings
        xmlversions = MetaDataSaved.versions2xmls(the_settings, self.versions)
        return MetaDataSaved(the_settings, xmlversions, self.params)

    def forward(self, context: Context) -> Iterable[BaseOperation]:
        migration = Migration(context)
        if self.metadata1 is None: migration.set_metadata1_none(context)
        else: migration.set_metadata1(context, self.metadata1)
        for delayed_operation in self._do_migrate(context, migration):
            yield delayed_operation

    def backward(self, context: Context) -> Iterable[BaseOperation]:
        migration = Migration(context)
        if self.metadata0 is None: migration.set_metadata1_none(context)
        else: migration.set_metadata1(context, self.metadata0)
        for delayed_operation in self._do_migrate(context, migration):
            yield delayed_operation

    def _do_migrate(self, context: Context, migration: Migration) -> Iterable[BaseOperation]:
        forward, backward = migration.direction()
        if forward and backward:
            raise self.exctype('Can be forward and backward in the same time.')
        if forward: member = migration.do_operations_forward
        elif backward: member = migration.do_operations_backward
        else: return
        for delayed_operation in member(context): yield delayed_operation
