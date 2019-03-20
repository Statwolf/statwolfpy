from unittest import TestCase
from unittest.mock import MagicMock, call

from statwolf.services import sql
from statwolf.services.sql import SQL

from statwolf.mocks import ContextMock, ResponseMock

class SQLFactoryTestCase(TestCase):

    def test_itShouldCreateAnSQLObject(self):
        context = ContextMock()

        s = sql.create(context)

        self.assertIsInstance(s, SQL)
        self.assertEqual(s._context, context)

class SQLTestCase(TestCase):

    def test_itShouldSendQueryCommands(self):
        context = ContextMock()

        data = {
            "meta": [{
                "name": "1",
                "type": "UInt8"
            }],
            "data": [{
                "1": 1
            }],
            "totals": None,
            "rows": 1,
            "rows_before_limit_at_least": 0,
            "hasErrors": False,
            "errorMessage": None
        }

        response = ResponseMock({
            "Success": True,
            "Data": {
                "data": data,
                "hints": {}
            }
        })
        context.http.post = MagicMock(return_value=response)

        sql = SQL(context)
        res = sql.query('select 1')

        context.http.post.assert_called_with('/root/v1/freehandquery/$$base', {
            'table': 'fhq_clickhouse_main',
            'query': {
                'keywords': {},
                'statement_clickhouse': 'select 1'
            }
        })

        self.assertEqual(res, data)
