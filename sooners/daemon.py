from argparse import ArgumentParser, Namespace
from collections import defaultdict
from datetime import date, datetime
from glob import glob
from os import close, dup2, execl, fdopen, fork, getpid
from os import kill, pipe, rename, waitpid, WNOHANG
from signal import signal, SIGINT, SIGTERM
from sys import stdout, stderr
from setproctitle import setproctitle
from .settings import the_settings
from .command import BaseCommand
from .component import BaseComponentFilePyClass, BaseComponentSubDirPyClass

class LogSwitch(object):
    start_mark, end_mark = '<ls<', '>ls>'
    @classmethod
    def parse(cls, line: str):
        if not line.startswith(cls.start_mark): return None
        if cls.end_mark not in line: return None
        key = line[len(cls.start_mark): line.index(cls.end_mark)]
        body = line[line.index(cls.end_mark) + len(cls.end_mark): ]
        return cls(key, body)
    def __init__(self, key: str, body: str):
        self.key, self.body = key, body
    def format(self) -> str:
        return '%s%s%s%s' % (self.start_mark, self.key, self.end_mark, self.body)

class BaseDaemonCommand(BaseCommand):
    del_mark = 'DELETE'
    class exctype(BaseCommand.exctype): pass
    @classmethod
    def scan_daemon_commands(cls, groups, settings):
        daemon_map = defaultdict(lambda: defaultdict(list))
        for component in settings.components.values():
            for command_name, command_class in cls.scan_in_subdir(component):
                daemon_group = getattr(command_class, 'daemon_group', None)
                daemon_priority = getattr(command_class, 'daemon_priority', 0)
                if daemon_group is None or daemon_group in groups:
                    daemon_map[daemon_group][daemon_priority].append(command_class)
        return daemon_map

    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('-s', '--stop', action = 'store_true',
                            help = 'stop the exist daemon.')
        parser.add_argument('-r', '--reload', action = 'store_true',
                            help = 'stop the exist daemon and start a new one.')
        parser.add_argument('-d', '--daemon', action = 'store_true',
                            help = 'run in daemon mode.')

    def handle(self, namespace: Namespace) -> object:
        super().handle(namespace)
        if getattr(self, 'logprefix', None) is None:
            self.logprefix = self.__module__.split('.')[-1]
        if namespace.stop or namespace.reload: self.stop_current()
        if namespace.stop: return
        if not namespace.daemon: return self.daemon_handle(namespace)
        if (subpid := fork()) > 0: return
        self.gzip_subpids = list()
        signal(SIGTERM, self.sigterm_func)
        setproctitle('g.%s' % self.logprefix)
        self.lnhdr = 'GUARDIAN(%u)' % getpid()
        self.logopen(self.nowstr())
        while True:
            rpipe, wpipe = pipe()
            subpid = fork()
            if subpid == 0: self.worker(rpipe, wpipe, namespace) # subprocess.
            elif self.guardian(rpipe, wpipe, subpid): continue
            else: break
        self.logclose()
        exit(0)

    def guardian(self, rpipe: int, wpipe: int, subpid: int) -> bool:
        close(wpipe)
        rfp, last_write_date, lsdict = fdopen(rpipe), self.curdate(), dict()
        self.logwrite_x('subprocess %u forked at %s.' % (subpid, self.nowstr()))
        while True:
            func = lambda subpid: waitpid(subpid, WNOHANG)[0] == 0
            self.gzip_subpids = list(filter(func, self.gzip_subpids))
            try:
                if not (line := rfp.readline()): break
                lsobj, this_write_date = LogSwitch.parse(line), self.curdate()
                if isinstance(lsobj, LogSwitch):
                    if lsobj.body.strip() != self.del_mark: lsdict[lsobj.key] = line
                    elif lsobj.key in lsdict: del(lsdict[lsobj.key])
                    else: self.logwrite(line)
                elif last_write_date < this_write_date:
                    self.logswitch(lsdict, 'it is another date %r' % this_write_date)
                    last_write_date = this_write_date
                    self.logwrite(line)
                elif self.wfp.tell() > the_settings.log_limit:
                    self.logswitch(lsdict, 'log_limit %u reached' % the_settings.log_limit)
                    self.logwrite(line)
                else: self.logwrite(line)
            except KeyboardInterrupt as exc:
                try: kill(subpid, SIGINT)
                except OSError as exc: reason = repr(exc)
                else: reason = 'succ'
                self.logwrite_x('kill(%u, %r): %s.' % (subpid, SIGINT, reason))
                break
        pid, status = waitpid(subpid, 0)
        self.logwrite_x('subprocess %u exited with %r at %s.' %
                        (subpid, (pid, status), self.nowstr()))
        rfp.close()
        if status == 0: return False # exit for common stop.
        elif status == 256: return False # exception raised, exit for debug.
        elif status == 65280: return False # for SIGINT from subpid.
        return True # reload it.

    def worker(self, rpipe: int, wpipe: int, namespace: Namespace):
        self.wfp.close()
        close(rpipe)
        setproctitle('w.%s' % self.logprefix)
        dup2(wpipe, stdout.fileno())
        dup2(wpipe, stderr.fileno())
        close(wpipe)
        try: exit_code = self.daemon_handle(namespace)
        except KeyboardInterrupt as exc:
            print('KeyboardInterrupt encountered at %s.' % self.nowstr())
            exit_code = 0
        exit(exit_code)

    def logopen(self, nowstr: str) -> None:
        self.logfname = '%s.%s.%u.log' % (self.logprefix, nowstr, getpid())
        self.wfp = the_settings.logs_dir.joinpath(self.logfname).open('wt')

    def logswitch(self, lsdict: dict, prompt: str) -> None:
        nowstr = self.nowstr()
        self.logwrite_x('%s at %s.' % (prompt, nowstr))
        self.logclose(); self.logopen(nowstr)
        for lsline in lsdict.values(): self.logwrite(lsline)

    def logwrite(self, msg: str) -> None:
        self.wfp.write(msg)
        self.wfp.flush()

    def logwrite_x(self, msg: str) -> None:
        return self.logwrite('%s: %s\n' % (self.lnhdr, msg))

    def logclose(self) -> None:
        logpath0 = the_settings.logs_dir.joinpath(self.logfname)
        logpath1 = the_settings.closed_logs_dir.joinpath(self.logfname)
        self.wfp.write('%s: exit: %s\n%s:     : %s\n' %
                       (self.lnhdr, logpath0.relative_to(the_settings.sandbox_root),
                        self.lnhdr, logpath1.relative_to(the_settings.sandbox_root)))
        self.wfp.close()
        rename(logpath0, logpath1)
        gzip_subpid = fork()
        if gzip_subpid == 0: execl('/bin/gzip', '/bin/gzip', '-9', logpath1)
        elif gzip_subpid > 0: self.gzip_subpids.append(gzip_subpid)

    def sigterm_func(self, signum, frame):
        raise KeyboardInterrupt('SIGTERM received.')

    def stop_current(self) -> None:
        for logfpath in the_settings.logs_dir.glob('%s.*.log' % self.logprefix):
            guardian_pid = int(logfpath.name.split('.')[3])
            try: kill(guardian_pid, SIGTERM)
            except ProcessLookupError as exc:
                print('kill(%u, %r): %r' % (guardian_pid, SIGTERM, exc))
                logfpath_backup = the_settings.closed_logs_dir.joinpath(logfpath.name)
                rename(logfpath, logfpath_backup)
                print('move: %r -> %r' % (
                    logfpath.relative_to(the_settings.sandbox_root),
                    logfpath_backup.relative_to(the_settings.sandbox_root)))
            except OSError as exc:
                print('kill(%u, %r): %r' % (guardian_pid, SIGTERM, exc))
            else: print('kill(%u, %r): succ.' % (guardian_pid, SIGTERM))

    def curdate(self) -> date: return datetime.now().date()
    def nowstr(self) -> str: return datetime.now().strftime('%Y%m%d-%H%M%S.%f')

    def daemon_handle(self, namespace: Namespace) -> object:
        raise NotImplementedError('daemon_handle must be overloaded by subclass.')

    def add_logswitch(self, key, body) -> str: return LogSwitch(key, body).format()
    def del_logswitch(self, key) -> str: return LogSwitch(key, self.del_mark).format()

class BaseDaemonWithScanCommand(BaseDaemonCommand):
    component_prefix = False
    def daemon_handle(self, namespace: Namespace):
        self.ctx_map_map = defaultdict(dict)
        for base_class_type in (BaseComponentFilePyClass, BaseComponentSubDirPyClass):
            func0 = lambda base_class: issubclass(base_class, base_class_type)
            for base_class in filter(func0, self.base_classes):
                for component in the_settings.components.values():
                    func1 = lambda ctx: (
                        base_class.make_map_name(ctx.component, ctx.name), ctx)
                    ctxlist = base_class.scan_component(component)
                    self.ctx_map_map[base_class.__name__].update(map(func1, ctxlist))
        return self.async_handle(namespace)

    def make_coroutines(self, namespace: Namespace):
        for base_class in self.base_classes:
            member = getattr(self, '%s_main' % base_class.__name__.lower()[len('Base'):])
            yield member(self.ctx_map_map[base_class.__name__], namespace)
