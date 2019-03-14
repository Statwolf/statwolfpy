from unittest import TestCase
from unittest.mock import MagicMock

import statwolf
from statwolf import Context
from statwolf.http import Http
from statwolf.tempfile import TempFile
from itertools import islice

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

    def test_itCallsTempFileFactory(self):
        ctx = Context(config())
        f = ctx.tempFile()
        self.assertIsInstance(f, TempFile)
        f.close()

    def test_itExtendsTheUrlWithDashboardBase(self):
        ctx = Context(config())
        self.assertEqual(ctx.toDashboard('/a path'), '/dashboard/path/a path')

    def test_itShouldWrapOpenFileFunction(self):
        ctx = Context(config())
        self.assertEqual(ctx.openFile, open)

    def test_itShuldWrapIsliceFunction(self):
        ctx = Context(config())
        self.assertEqual(ctx.islice, islice)

    def test_itShouldHaveAFactoryForBlobService(self):
        class BlobConfig:
            def __init__(self):
                self.config = {
                    "Success": True,
                    "Data": {
                        "connectionString": "the connection string",
                        "baseUrl": "the base url"
                    }
                }
            def json(self):
                return self.config

        service = {}
        blobConfig = BlobConfig()
        ctx = Context(config())
        ctx._blockBlobService = MagicMock(return_value=service)
        ctx.http.post = MagicMock(return_value=blobConfig)

        b, baseUrl = ctx.blob()

        ctx.http.post.assert_called_with('/dashboard/path/v1/datasetimport/env')
        ctx._blockBlobService.assert_called_with(connection_string="the connection string")
        self.assertEqual(b, service)
        self.assertEqual(baseUrl, 'the base url')

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
