from statwolf.services.baseservice import BaseService
from statwolf import StatwolfException
from copy import deepcopy

import json

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

        res = self.post(self._baseUrl, params)

        if 'Code' in res:
            error = json.loads(res['Message'].replace('Error: ', ''))
            raise StatwolfException(json.loads(error['response'])['message'])

        return { key: res['data'][key] for key in ['data', 'meta'] }

def create(context):
    return SQL(context)
