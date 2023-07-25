from .component import BaseComponentFilePyClass

class BaseTestCase(BaseComponentFilePyClass):
    component_filename = 'testcases'
    class exctype(Exception): pass
