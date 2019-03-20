from statwolf.services.baseservice import BaseService
from copy import deepcopy

class SQL(BaseService):

    def __init__(self, context):
        super(SQL, self).__init__(context)

        self._baseUrl = context.toDashboard('/v1/freehandquery/$$base')
        self._baseParams = {
            "table": "fhq_clickhouse_main",
            "query": {
                "keywords": {}
            }
        }

    def query(self, statement):
        params = deepcopy(self._baseParams)
        params['query']['statement_clickhouse'] = statement

        return self.post(self._baseUrl, params)['data']

def create(context):
    return SQL(context)
