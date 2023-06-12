from argparse import ArgumentParser, Namespace
from ...command import BaseCommand

class Command(BaseCommand):
    help = ('make the patch from the specified versions.')
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument('component', type = str,
                            choices = self.settings.components.keys(),
                            help = 'Specify the component to be patched.')
        parser.add_argument('version0', type = int)
        parser.add_argument('version1', type = int)

    def handle(self, namespace: Namespace) -> None:
        from ..db.metadata import make_patch
        super().handle(namespace)
        component = self.settings.components[namespace.component]
        xmlpatch = make_patch(component.version_parse(namespace.version0),
                              component.version_parse(namespace.version1), self.prompt)
        component.patch_write(xmlpatch, namespace.version0, namespace.version1)
        patch_fname = component.patch_fname(namespace.version0, namespace.version1)
        self.prompt('%r: written for %r.' % (patch_fname, component), opts = ('bold',))
