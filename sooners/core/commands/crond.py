from argparse import Namespace
from asyncio import sleep as aiosleep
from datetime import datetime, timedelta
from ...cron import BaseCronTask
from ...daemon import BaseDaemonWithScanCommand

class Command(BaseDaemonWithScanCommand):
    help = 'run the cron tasks.'
    base_classes = [BaseCronTask]
    async def crontask_main(self, class_map: dict, namespace: Namespace):
        func = lambda cron: (cron[0], cron[1](namespace))
        cron_map = dict(map(func, class_map.items()))
        while cron_map:
            now_at = datetime.now()
            if now_at.second == 59: fix = 1000000 - now_at.microsecond
            elif now_at.second == 0: fix = -now_at.microsecond
            elif now_at.microsecond < 500000: fix = -now_at.microsecond
            else: fix = 1000000 - now_at.microsecond
            now_at += timedelta(seconds = fix / 1000000)
            if now_at.second > 0: pass
            else:
                func = lambda cron: (*cron, cron[1].launch_ornot(now_at))
                for cron_name, cron_object, plan_at in filter(func, cron_map.items()):
                    if plan_at is None: continue
                    elif cron_object.busy():
                        print('Discard the overlapped launch: %r(%r)...' %
                              (cron_object, plan_at))
                    else:
                        print('Launch: %r(%r)...' % (cron_object, next_plan_at))
                        cron_object.launch_work(cron_name, plan_at, now_at)
            func = lambda cron: cron[1].next_plan_at(now_at)
            next_plan_at = min(map(func, cron_map.items()))
            if next_plan_at < now_at: continue
            sleep_period = next_plan_at - now_at
            print('Sleep: %r...' % sleep_period)
            await aiosleep(sleep_period.seconds)
