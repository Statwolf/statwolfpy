class ResponseMock:
    def __init__(self, defaultReply={"Success": True,"Data": {}}):
        self.DEFAULT_REPLY = defaultReply

    def json(self):
        return self.DEFAULT_REPLY

class HttpMock:
    def post(self, path, body):
        pass

    def get(self, path):
        pass

class FileMock:
    def write(self):
        pass

    def close(self):
        pass

    def location(self):
        return "file_location/file_name"

class BlobServiceMock:
    def create_blob_from_path(self):
        pass

class ContextMock:
    def __init__(self):
        self.http = HttpMock()
        self._fileMock = FileMock()
        self._blob = BlobServiceMock()

    def toDashboard(self, url):
        return '/root' + url

    def tempFile(self):
        return self._fileMock

    def blob(self):
        return self._blob, 'base url/'

class DatasourceMock:
    def explore(self, sourceid):
        pass
