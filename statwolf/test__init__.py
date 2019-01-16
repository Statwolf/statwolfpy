from unittest import TestCase
from unittest.mock import MagicMock

import statwolf
from statwolf import Context
from statwolf.http import Http

def config():
    return {
        "host": "https://a.statwolf.endpoint/dashboard/path",
        "username": "the username",
        "password": "a real password"
    }

class ContextTestCase(TestCase):

    def test_itShouldSplitTheBaseUrl(self):
        ctx = Context(config())
        self.assertEqual(ctx.config['host'], 'https://a.statwolf.endpoint')
        self.assertEqual(ctx.config['root'], '/dashboard/path')

    def test_itShouldSaveTheCurrentConfig(self):
        ctx = Context(config())
        self.assertEqual(ctx.config, {
            "host": "https://a.statwolf.endpoint",
            "root": "/dashboard/path",
            "username": "the username",
            "password": "a real password"
        })

    def test_itShouldLoadDeps(self):
        ctx = Context(config())
        self.assertEqual(ctx.loader, getattr)
        self.assertIs(type(ctx.http), Http)

    def test_itExtendsTheUrlWithDashboardBase(self):
        ctx = Context(config())
        self.assertEqual(ctx.toDashboard('/a path'), '/dashboard/path/a path')

class InitTestCase(TestCase):

    def test_itShouldCreateTheMainService(self):
        class Service:
            pass
        fakeModule = Service()
        fakeModule.create = MagicMock(return_value=42)
        class Context:
            pass
        context = Context()
        context.loader = MagicMock(return_value=fakeModule)

        service = statwolf._internal_create(context, "fake_service")
        self.assertIs(service, 42)
        context.loader.assert_called_with(statwolf.services, 'fake_service')
        fakeModule.create.assert_called_with(context)

    def test_itShouldCheckTheMandatoryConfigFields(self):
        with self.assertRaises(statwolf.InvalidConfigException):
            config = {}
            statwolf.create(config, "fake_service")
