from unittest import TestCase
from unittest.mock import MagicMock

from statwolf.services import datasource
from statwolf.services.datasource import Datasource, DatasourceInstance

from statwolf.mocks import ContextMock, ResponseMock

class DatasourceFactoryTestCase(TestCase):

    def test_itShouldCreateTheDatasourceService(self):
        context = {}
        d = datasource.create(context)

        self.assertIs(type(d), Datasource)
        self.assertEqual(d._context, context)

class DatasourceInstanceTestCase(TestCase):

    def setUp(self):
        self.context = ContextMock()

        self.data = {
            "schema": { "field": "the schema" },
            "dimensions": [ "the dimensions" ],
            "metrics": [ "the metrics" ],
            "filters": [ "the filters" ]
        }

        self.response = ResponseMock({
            "Success": True,
            "Data": self.data
        })
        self.context.http.post = MagicMock(return_value=self.response)

    def test_itShouldListTheSourceIds(self):
        mock_list = [ "yolo", "yolt" ]

        self.response = ResponseMock({
            "Success": True,
            "Data": mock_list
        })
        self.context.http.post = MagicMock(return_value=self.response)

        d = datasource.create(self.context)

        the_list = d.list()
        self.assertEqual(the_list, mock_list)
        self.context.http.post.assert_called_with('/root/v1/full/listSchemas', {})

    def test_itShouldLoadMetadataOnCreation(self):
        d = datasource.create(self.context)
        instance = d.explore('mock source')

        self.context.http.post.assert_called_with('/root/v1/full/getSchema', {
            "sourceid": "mock source"
        })
        self.assertEqual(instance._meta, self.response.json()["Data"])

    def test_itShouldReturnTheMetaFields(self):
        d = datasource.create(self.context)
        instance = d.explore('mock source')

        self.assertEqual(instance.schema(), self.data["schema"])
        self.assertEqual(instance.dimensions()[0].name(), self.data["dimensions"][0])
        self.assertEqual(instance.metrics()[0].name(), self.data["metrics"][0])
        self.assertEqual(instance.filters()[0].name(), self.data["filters"][0])
        self.assertEqual(instance.filter("the filters").name(), self.data["filters"][0])
        self.assertEqual(instance.filter("invalid"), None)
        self.assertEqual(instance.raw(), self.data)

    def test_itShouldGetHintsForAField(self):
        d = datasource.create(self.context)
        instance = d.explore('mock source')

        f = instance.filters()[0]

        data = {
            "Success": True,
            "Data": [ "values" ]
        }
        self.context.http.post = MagicMock(return_value=ResponseMock(data))

        res = f.values("test")

        self.context.http.post.assert_called_with("/root/v1/full/getHints", {
            "field": "the filters",
            "text": "test",
            "table": "mock source"
        })

        self.assertEqual(res, data["Data"])
