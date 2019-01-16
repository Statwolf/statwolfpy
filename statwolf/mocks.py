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

class ContextMock:
    def __init__(self):
        self.http = HttpMock()

    def toDashboard(self, url):
        return '/root' + url

class DatasourceMock:
    def explore(self, sourceid):
        pass
