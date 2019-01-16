from unittest import TestCase
from unittest.mock import MagicMock

from statwolf.services import statwolfml
from statwolf.services.statwolfml import StatwolfML

from statwolf.mocks import ContextMock, ResponseMock

class StatwolfMLFactoryTestCase(TestCase):

    def test_itShouldCreateTheStatwolfMLService(self):
        context = {}
        ml = statwolfml.create(context)

        self.assertIs(type(ml), StatwolfML)
        self.assertEqual(ml._context, context)

class StatwolfMLTestCase(TestCase):

    def test_itCallsThePreprocessEndpoint(self):
        context = ContextMock()

        response = ResponseMock()
        context.http.post = MagicMock(return_value=response)

        anInput = {}

        uut = statwolfml.create(context)
        result = uut.preprocess(anInput)

        context.http.post.assert_called_with('/v1/statwolfml/preprocess', anInput)
        self.assertEqual(result, response.DEFAULT_REPLY["Data"])

    def test_itCallsTheApplyEndpoint(self):
        context = ContextMock()

        response = ResponseMock()
        context.http.post = MagicMock(return_value=response)

        model = { "model": "model" }
        config = { "config": "config" }

        uut = statwolfml.create(context)
        result = uut.apply(model, config)

        context.http.post.assert_called_with('/v1/statwolfml/apply', {
            "model": model,
            "config": config
        })
        self.assertEqual(result, response.DEFAULT_REPLY["Data"])
