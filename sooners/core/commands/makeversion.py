from argparse import ArgumentParser, Namespace
from ...command import BaseCommand
from ...component import BaseComponent

class Command(BaseCommand):
    help = ('save the current database schema version for each component.')
    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        self.cnames = tuple(self.settings.components.keys())
        parser.add_argument('component', nargs = '*', choices = ([], *self.cnames),
                            help = 'Specify the component to be saved.')

    def handle(self, namespace: Namespace) -> None:
        super().handle(namespace)
        self.settings.load_models()
        cnames = namespace.component if namespace.component else self.cnames
        for component in self.settings.components.values():
            if component.name not in cnames: continue
            self.one_component(component, namespace)

    def one_component(self, component: BaseComponent, namespace: Namespace) -> None:
        if (xmlversion := self.settings.metadata.make_version(component)) is None: return
        checksum = xmlversion.getAttribute('checksum')
        xmlversions = component.version_parse_all()
        func = lambda xmlversion: xmlversion.getAttribute('checksum') == checksum
        if bool(matched_xmlversions := tuple(filter(func, xmlversions))):
            assert(len(matched_xmlversions) == 1)
            version = int(matched_xmlversions[0].getAttribute('version'))
            version_fname = component.version_fname(version)
            self.prompt('%r: matched for %r.' % (version_fname, component))
        elif not xmlversions:
            version = 1
            component.version_write(xmlversion, version)
            version_fname = component.version_fname(version)
            self.prompt('%r: written for %r.' % (version_fname, component), opts = ('bold',))
        else:
            from ...db.metadata import make_patch
            last_version = int(xmlversions[-1].getAttribute('version'))
            version = last_version + 1
            component.version_write(xmlversion, version)
            version_fname = component.version_fname(version)
            self.prompt('%r: written for %r.' % (version_fname, component), opts = ('bold',))
            xmlpatch = make_patch(xmlversions[-1], xmlversion, self.prompt)
            component.patch_write(xmlpatch, last_version, version)
            patch_fname = component.patch_fname(last_version, version)
            self.prompt('%r: written for %r.' % (patch_fname, component), opts = ('bold',))
