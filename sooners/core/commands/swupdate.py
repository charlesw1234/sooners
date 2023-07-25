from argparse import Namespace
from ...command import BaseCommand
from ...component import BaseComponent

class Command(BaseCommand):
    help = ('update the shard weight for every shard entity model.')
    def handle(self, namespace: Namespace) -> None:
        from ..models import ShardWeight
        super().handle(namespace)
        updated = dict()
        self.settings.load_models()
        context = self.settings.make_db_context()
        func = lambda weight_record: ((weight_record.name, weight_record.suffix), weight_record)
        wrdict = dict(map(func, context.default.session.query(ShardWeight)))
        for entity_model in self.settings.metadata.models.shard_entity.values():
            key = (entity_model.__shard_name__, entity_model.__shard_suffix__)
            assert(len(entity_model.__database_names__) == 1)
            database_name = tuple(entity_model.__database_names__)[0]
            count = context.sessions[database_name].query(entity_model).count()
            if key not in wrdict :
                wrdict[key] = ShardWeight(
                    name = entity_model.__shard_name__,
                    suffix = entity_model.__shard_suffix__,
                    count = count)
                context.default.session.add(wrdict[key])
                updated[key] = count
            elif wrdict[key].count != count:
                wrdict[key].count = count
                updated[key] = count
        context.default.session.commit()
        for key, count in updated.items():
            self.prompt('%s_%s: %u' % (*key, count))
