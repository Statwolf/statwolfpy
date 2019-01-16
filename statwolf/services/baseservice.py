class BaseService:

    def __init__(self, context):
        self._context = context

    def post(self, path, body):
        result = self._context.http.post(path, body)

        return result.json()["Data"]
