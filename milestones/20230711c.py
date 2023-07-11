from sooners.utils import Context
from sooners.settings import the_settings
from sooners.milestone import Milestone
from sooners.milestone.dbschema import DBSchemaStep

milestone = Milestone(
    '20230711b',
    DBSchemaStep(
        Context(sooners_core=1, sooners_sample1=2),
        Context(sample1_building=Context(database_names={'test0'}),
                sample1_point=Context(database_names={
                    'test0': ['000'],
                    'test2': ['003', '004', '005', '006', '007', '008'],
                    'test1': ['001', '002']}),
                sooners_configuration=Context(database_names={'test0'}),
                sooners_dbschema_version=Context(database_names={'test0'}),
                sooners_dbschema_operation=Context(database_names={'test0', 'test2', 'test1'}),
                sooners_shard_weight=Context(database_names={'test0'}),
                sample1_floor=Context(database_names={'test0'})))
)
