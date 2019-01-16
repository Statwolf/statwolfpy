from unittest import TestCase
from unittest.mock import MagicMock

from statwolf.services import statwolfml
from statwolf.services.baseservice import BaseService

from statwolf.mocks import ContextMock, ResponseMock

class StatwolfMLFactoryTestCase(TestCase):

    def test_itShouldCreateTheStatwolfMLService(self):
        context = {}
        bs = BaseService(context)
        self.assertEqual(bs._context, context)

    def test_itShouldParseTheStatwolfReplyOnRequest(self):
        context = ContextMock()
        response = ResponseMock();

        context.http.post = MagicMock(return_value=response)

        bs = BaseService(context)

        body = {}
        self.assertEqual(bs.post('a path', body), response.DEFAULT_REPLY["Data"])
        context.http.post.asser_called_with('a path', body)
