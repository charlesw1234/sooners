from ...utils import Context
from .libs.milestonebase import BaseMilestoneCommand

class Command(BaseMilestoneCommand):
    help = ('do milestone steps in backward direction.')
    def sub_handle(self, milestone, patterns, context: Context) -> None:
        milestone.backward(patterns, context)
