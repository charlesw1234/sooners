class SourceVersion(object):
    @classmethod
    def parse(cls, representation: str):
        name, version_str = representation.split('-')
        major_str, minor_str = version_str.split('.')
        return cls(name, int(major_str), int(minor_str))
    def __init__(self, name: str, major: int, minor: int) -> None:
        self.name, self.major, self.minor = name, major, minor
    def __repr__(self): return '%s-%02u.%02u' % (self.name, self.major, self.minor)
    def __eq__(self, other):
        assert(self.name == other.name)
        return self.major == other.major and self.minor == other.minor
    def __ne__(self, other): return not self.__eq__(other)
    def __lt__(self, other):
        assert(self.name == other.name)
        if self.major < other.major: return True
        return self.major == other.major and self.minor < other.minor
    def __gt__(self, other):
        assert(self.name == other.name)
        if self.major > other.major: return True
        return self.major == other.major and self.minor > other.minor
    def __le__(self, other): return not self.__gt__(other)
    def __ge__(self, other): return not self.__lt__(other)
    def compatible(self, other) -> bool:
        assert(self.name == other.name)
        return self.major == other.major

source_version = SourceVersion('sooners', 0, 0)
