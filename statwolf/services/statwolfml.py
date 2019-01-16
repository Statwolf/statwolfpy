from statwolf.services.baseservice import BaseService

class StatwolfML(BaseService):
    def __init__(self, context):
        super(StatwolfML, self).__init__(context)

        self._basePath = '/v1/statwolfml'

    def preprocess(self, config):
        path = self._basePath + '/preprocess'

        return self.post(path, config)

    def apply(self, model, config):
        path = self._basePath + '/apply'

        return self.post(path, {
            "model": model,
            "config": config
        })


def create(context):
    return StatwolfML(context)
