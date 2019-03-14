import tempfile
import gzip
import os

class TempFile:

    def __init__(self, tempfile, openFile, removeFile):
        location = tempfile.mkstemp()[1]
        self._tmpFile = openFile(location, 'w')
        self._removeFile = removeFile

    def write(self, data):
        self._tmpFile.write(str.encode(data))

    def close(self):
        return self._tmpFile.close()

    def location(self):
        return self._tmpFile.name

    def remove(self):
        return self._removeFile(self.location())

    def create():
        return TempFile(tempfile, gzip.open, os.remove)
