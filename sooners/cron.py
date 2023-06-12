from datetime import date, datetime
from .component import BaseComponentSubDirPyClass

class BaseCronTask(BaseComponentSubDirPyClass):
    component_subdir = 'crons'
    component_object_name = 'CronTask'
    class exctype(Exception): pass
    def __init__(self):
        super().__init__()
        self.task = None

    def busy(self) -> bool:
        if self.task is None: return False
        assert(isinstance(self.task, asyncio.Task))
        return self.task._state == 'PENDING' and self.task.get_coro().cr_suspended

    def launch_work(self, name: str, plan_at: datetime, now_at: datetime) -> None:
        task_name = 'cron(%s@%r/%r)' % (name, plan_at, now_at)
        self.task = asyncio.create_task(self.work(name, plan_at, now_at), name = task_name)

    async def await_work(self) -> object:
        assert(isinstance(self.task, asyncio.Task))
        assert(self.task._state == 'FINISHED')
        return await self.task

    async def work(self, name: str, plan_at: datetime, now_at: datetime) -> object:
        pass # fixme.

    @classmethod
    def post_setup(cls):
        if not hasattr(cls, 'cron_monthes'): cls.cron_monthes = range(1, 13)
        if not hasattr(cls, 'cron_weekdays'): cls.cron_weekdays = range(7)
        if not hasattr(cls, 'cron_hours'): cls.cron_hours = range(24)
        if not hasattr(cls, 'cron_minutes'): cls.cron_minutes = range(60)
        return cls

    @classmethod
    def get_cron_days(cls, now_at):
        if now_at.month in (1, 3, 5, 7, 8, 10, 12): lastday = 31
        elif now_at.month in (4, 6, 9, 11): lastday = 30
        else: lastday = (date(now_at.year, 3, 1) - date(now_at.year, 2, 1)).days
        func = lambda day: day <= lastday
        return filter(func, getattr(cls, 'cron_days', range(1, 32)))

    @classmethod
    def launch_ornot(cls, now_at: datetime) -> datetime | None:
        assert(now_at.microsecond == 0)
        if now_at.second != 0: return None
        if now_at.minute not in cls.cron_minutes(): return None
        elif now_at.hour not in cls.cron_hours(): return None
        elif now_at.weekday() not in cls.cron_weekdays(): return None
        elif now_at.day not in cls.get_cron_days(now_at): return None
        elif now_at.month not in cls.cron_monthes(now_at): return None
        return now_at

    @classmethod
    def next_plan_at(cls, now_at: datetime) -> datetime:
        while True:
            if now_at.month in cls.cron_monthes and\
               (next_plan_at := cls._next_plan_at_day(now_at)) is not None:
                return next_plan_at
            next_month = cls._first_bigger(now_at.month, cls.cron_monthes)
            if next_month is None:
                now_at = datetime(now_at.year + 1, cls.cron_monthes[0], 1, 0, 0, 0)
            else: now_at = datetime(now_at.year, next_month, 1, 0, 0, 0)

    @classmethod
    def _next_plan_at_day(cls, now_at: datetime) -> datetime | None:
        cron_days = tuple(cls.get_cron_days(now_at))
        while True:
            if now_at.day in cron_days and now_at.weekday() in cls.cron_weekdays and\
               (next_plan_at := cls._next_plan_at_hour(now_at)) is not None:
                return next_plan_at
            while True:
                next_day = cls._first_bigger(now_at.day, cron_days)
                if next_day is None: return None
                now_at = datetime(now_at.year, now_at.month, next_day, 0, 0, 0)
                if now_at.weekday() in cls.cron_weekdays: break

    @classmethod
    def _next_plan_at_hour(cls, now_at: datetime) -> datetime | None:
        while True:
            if now_at.hour in cls.cron_hours and\
               (next_plan_at := cls._next_plan_at_minute(now_at)) is not None:
                return next_plan_at
            next_hour = cls._first_bigger(now_at.hour, cls.cron_hours)
            if next_hour is None: return None
            now_at = datetime(now_at.year, now_at.month, now_at.day, next_hour, 0, 0)

    @classmethod
    def _next_plan_at_minute(cls, now_at: datetime) -> datetime | None:
        next_minute = cls._first_bigger(now_at.minute, cls.cron_minutes)
        if next_minute is None: return None
        return datetime(now_at.year, now_at.month, now_at.day,
                        now_at.hour, next_minute, 0)

    @classmethod
    def _first_bigger(cls, value0, values):
        for value1 in filter(lambda value1: value0 < value1, values): return value1
        return None
