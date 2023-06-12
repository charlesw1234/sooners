from fnmatch import fnmatch
from typing import Iterable
from ..utils import Context

class BaseMilestoneStep(object):
    class exctype(Exception): pass
    def __init__(self) -> None:
        self.index = None
        func = lambda ch: ch.isupper()
        self.abbrev = (''.join(filter(func, self.__class__.__name__))).lower()
    def __repr__(self) -> str: return '%s.%s.%s%s' % (
            self.abbrev,
            'xx' if self.index is None else'%02u' % self.index,
            'f' if self.can_forward() else 'x',
            'b' if self.can_backward() else 'x')

    def can_forward(self) -> bool: return callable(getattr(self, 'forward', None))
    def can_backward(self) -> bool: return callable(getattr(self, 'backward', None))

    def description(self) -> str: return ''

    def setup_by_milestone(self, milestone, index: int, width: int):
        self.milestone, self.index = milestone, index
        self.abbrev = ' ' * (width - len(self.abbrev)) + self.abbrev

    @property
    def metadata0(self):
        if not hasattr(self, '_metadata0'):
            assert(self.index is not None)
            if self.index > 0: previous_step = self.milestone[self.index - 1]
            elif self.milestone.previous_milestone is None: previous_step = None
            else: previous_step = self.milestone.previous_milestone[-1]
            self._metadata0 = None if previous_step is None else previous_step.metadata1
        return self._metadata0
    @property
    def metadata1(self):
        if not hasattr(self, '_metadata1'):
            if callable(getattr(self, 'setup_metadata1', None)):
                self._metadata1 = self.setup_metadata1()
            else: self._metadata1 = self.metadata0
        return self._metadata1

class MilestoneStepPatterns(object):
    def __init__(self, confirm_ornot: bool, *patterns: tuple[str]) -> None:
        self.confirm_ornot = confirm_ornot
        func = lambda pattern: pattern if '-' not in pattern else tuple(pattern.split('-'))
        self.patterns = tuple(map(func, patterns))

    def __repr__(self):
        return '%s%r' % (self.__class__.__name__, self.patterns)

    def match_patterns(self, step: BaseMilestoneStep) -> Iterable[str]:
        step_repr, scopes = repr(step), set()
        for index, pattern in enumerate(self.patterns):
            if isinstance(pattern, str):
                if fnmatch(step_repr, pattern): yield pattern
            elif isinstance(pattern, tuple):
                if fnmatch(step_repr, pattern[0]):
                    yield_ornot = True
                    scopes.add(index)
                if fnmatch(step_repr, pattern[-1]):
                    yield_ornot = True
                    if index in scopes: scopes.remove(index)
                if yield_ornot or index in scopes: yield '-'.join(pattern)

    def match_ornot(self, step: BaseMilestoneStep) -> bool:
        return bool(tuple(self.match_patterns(step)))

    def match_and_confirm(self, step: BaseMilestoneStep, direction: str) -> tuple[str]:
        if not (matched := tuple(self.match_patterns(step))): return matched
        if not self.confirm_ornot: return matched
        answer = input('%r.%s: %r, confirm to do it(y/n)? ' % (step, direction, matched))
        return matched if answer.lower().startswith('y') else tuple()

class Milestone(list):
    def __init__(self, previous_milestone_name: str | None,
                 *steps: tuple[BaseMilestoneStep]) -> None:
        super().__init__(steps)
        self.previous_milestone_name = previous_milestone_name
        width = max(map(lambda step: len(step.abbrev), self))
        for index, step in enumerate(self): step.setup_by_milestone(self, index, width)

    @property
    def previous_milestone(self):
        if not hasattr(self, '_previous_milestone'):
            if self.previous_milestone_name is None: self._previous_milestone = None
            else:
                from importlib import import_module
                module = import_module('milestones.%s' % self.previous_milestone_name)
                self._previous_milestone = module.milestone
        return self._previous_milestone

    def show(self) -> Iterable[str]:
        for step in self: yield '%r: %s.' % (step, step.description())

    def forward(self, patterns: MilestoneStepPatterns, context: Context) -> None:
        func0 = lambda step: patterns.match_ornot(step)
        func1 = lambda step: not step.can_forward()
        banned = tuple(filter(func1, filter(func0, self)))
        if banned: raise context.exctype('%r can not do forward.' % banned)
        for step in self:
            if not (matched := patterns.match_and_confirm(step, 'forward')): continue
            if not context.do_action: continue
            context.prompt('%r.forward@%r: %s.' % (step, matched, step.description()))
            try: delayed_operations = tuple(step.forward(context))
            except step.exctype as exc: raise context.exctype(repr(exc))
            self.do_delayed_operations(context, delayed_operations)
            self.commit_all(context)

    def backward(self, patterns: MilestoneStepPatterns, context: Context) -> None:
        func0 = lambda step: patterns.match_ornot(step)
        func1 = lambda step: not step.can_backward()
        banned = tuple(filter(func1, filter(func0, reversed(self))))
        if banned: raise context.exctype('%r can not do backward.' % banned)
        for step in reversed(self):
            if not (matched := patterns.match_and_confirm(step, 'forward')): continue
            if not context.do_action: continue
            context.prompt('%r.backward@%r: %s.' % (step, matched, step.description()))
            try: delayed_operations = tuple(step.backward(context))
            except step.exctype as exc: raise context.exctype(repr(exc))
            self.do_delayed_operations(context, delayed_operations)
            self.commit_all(context)

    def do_delayed_operations(self, context: Context, delayed_operations) -> None:
        for delayed_operation in delayed_operations:
            delayed_operation(context.prompt,
                              context.operators[delayed_operation.database_name])

    def commit_all(self, context: Context) -> None:
        for session in context.sessions.values(): session.commit()
