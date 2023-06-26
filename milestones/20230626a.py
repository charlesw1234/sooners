from sooners.utils import Context
from sooners.settings import the_settings
from sooners.milestone import Milestone
from sooners.milestone.dbschema import DBSchemaStep

the_settings.load_models()
milestone = Milestone(
    None,
    DBSchemaStep(Context(sooners_core = 1, sooners_sample1 = 1),
                 the_settings.metadata.save_params())
)
