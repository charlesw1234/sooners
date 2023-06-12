from argparse import ArgumentParser, Namespace
from ...command import BaseCommand

class Command(BaseCommand):
    help = ('run a python interactive interpreter.')
    shells = ['python', 'ipython', 'bpython']
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            '-i', '--interface', choices = self.shells,
            help = 'Specify an interactive interpreter interface.')
        parser.add_argument('-c', '--command', help = 'run a command and exit.')

    def python(self, namespace: Namespace) -> None:
        import code, sys, traceback
        imported_objects = {}
        try: hook = sys.__interactivehook__
        except AttributeError as exc: traceback.print_exc()
        else:
            try: hook()
            except Exception as exc: traceback.print_exc()
        try:
            import readline
            import rlcompleter
        except Exception as exc: traceback.print_exc()
        else:
            readline.set_completer(rlcompleter.Completer(imported_objects).complete)
        code.interact(local = imported_objects)

    def ipython(self, namespace: Namespace) -> None:
        from IPython import start_ipython
        start_ipython(argv = [])

    def bpython(self, namespace: Namespace) -> None:
        import bpython
        bpython.embed()

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        if namespace.command:
            exec(namespace.command)
            return
        available_shells = [namespace.interface] if namespace.interface else self.shells
        for shell in available_shells:
            try: return getattr(self, shell)(namespace)
            except ImportError: pass
        raise self.exctype("Couldn't import %r interface." % (shell,))
