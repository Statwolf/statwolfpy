from unittest import TestCase
from unittest.mock import MagicMock

from statwolf.services import datasource
from statwolf.services.datasource import Datasource, DatasourceInstance, Upload, Parser, Blob

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

    def test_itShouldOpenAFileAsLineStream(self):
        f = {}
        self.context.openFile = MagicMock(return_value=f)

        u = Upload('yolo', 'label', self.context)
        p = u.file('local path')

        self.context.openFile.assert_called_with('local path', 'r')
        self.assertIsInstance(p, Parser)
        self.assertEqual(p._context, self.context)
        self.assertEqual(p._sourceid, 'yolo')
        self.assertEqual(p._label, 'label')
        self.assertEqual(p._source, f)

    def test_JsonParseShouldDONothing(self):
        s = {}
        p = Parser('yolo', 'label', s, self.context)
        b = p.json()

        self.assertIsInstance(b, Blob)
        self.assertEqual(b._sourceid, 'yolo')
        self.assertEqual(b._label, 'label')
        self.assertEqual(b._source, s)

        batch = [ 1, 2, 3 ]
        self.assertEqual(b._parser(batch), batch)

    def test_ParserShouldAllowCustomParsers(self):
        y = []
        def custom(x): return y

        s = {}
        p = Parser('yolo', 'label', s, self.context)
        b = p.custom(custom)

        self.assertIsInstance(b, Blob)
        self.assertEqual(b._sourceid, 'yolo')
        self.assertEqual(b._label, 'label')
        self.assertEqual(b._source, s)

        batch = [ 1, 2, 3 ]
        self.assertEqual(b._parser(batch), y)


