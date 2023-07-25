from typing import Iterable
from . import BaseMilestoneStep
from ..utils import Context
from ..db.operations import BaseOperation
from ..db.metadata import MetaDataSaved
from ..db.migration import Migration

class DBSchemaStep(BaseMilestoneStep):
    def __init__(self, versions: Context, params: Context):
        super().__init__()
        self.versions, self.params = versions, params

    def description(self): return '(%r)-(%r)' % (self.versions, self.params)

    def setup_metadata1(self) -> MetaDataSaved:
        from ..settings import the_settings
        xmlversions = MetaDataSaved.versions2xmls(the_settings, self.versions)
        return MetaDataSaved(the_settings, xmlversions, self.params)

    def make_migration(self, context: Context) -> Migration:
        migration = Migration(context)
        if migration.is_clean(): return migration
        raise RuntimeError('Use skcontinue or skwithdraw before this.')

    def forward(self, context: Context) -> Iterable[BaseOperation]:
        migration = self.make_migration(context)
        if self.metadata1 is None: migration.set_metadata1_none(context)
        else: migration.set_metadata1(context, self.metadata1)
        for delayed_operation in self._do_migrate(context, migration):
            yield delayed_operation

    def backward(self, context: Context) -> Iterable[BaseOperation]:
        migration = self.make_migration(context)
        if self.metadata0 is None: migration.set_metadata1_none(context)
        else: migration.set_metadata1(context, self.metadata0)
        for delayed_operation in self._do_migrate(context, migration):
            yield delayed_operation

    def _do_migrate(self, context: Context, migration: Migration) -> Iterable[BaseOperation]:
        member = migration.direction_member(self.exctype)
        for delayed_operation in member(context): yield delayed_operation
