from sooners.utils import Context
from sooners.settings import the_settings
from sooners.milestone import Milestone
from sooners.milestone.dbschema import DBSchemaStep

milestone = Milestone(
    None,
    DBSchemaStep(
        Context(sooners_core=1, sooners_sample1=1),
        Context(sample1_building=Context(database_names={'test0'}),
                sample1_point=Context(database_names={
                    'test0': ['000'],
                    'test1': ['001', '002'],
                    'test2': ['003', '004', '005', '006', '007', '008']}),
                sooners_configuration=Context(database_names={'test0'}),
                sooners_dbschema_version=Context(database_names={'test0'}),
                sooners_dbschema_operation=Context(database_names={'test0', 'test1', 'test2'}),
                sooners_shard_weight=Context(database_names={'test0'}),
                sample1_floor=Context(database_names={'test0'})))
)
