from unittest import TestCase
from unittest.mock import MagicMock, call

from statwolf.services import datasource
from statwolf.services.datasource import Datasource, DatasourceInstance, Upload, Parser, Blob, UploaderPanel
from statwolf import StatwolfException

from statwolf.mocks import ContextMock, ResponseMock, FileMock

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

    def test_itShouldCreateAnUploadObject(self):
        d = Datasource(self.context)
        u = d.upload('sourceid', 'label')

        self.assertIsInstance(u, Upload)
        self.assertEqual(u._sourceid, 'sourceid')
        self.assertEqual(u._label, 'label')
        self.assertEqual(u._context, self.context)

    def test_uploadShouldCreateAParserObject(self):
        handler = {}

        u = Upload('sourceid', 'label', self.context)
        p = u.source(handler)

        self.assertIsInstance(p, Parser)
        self.assertEqual(p._sourceid, 'sourceid')
        self.assertEqual(p._label, 'label')
        self.assertEqual(p._source, handler)
        self.assertEqual(p._context, self.context)

    def test_parserShouldCreateAnUploadObject(self):
        p = Parser('sourceid', 'label', 'handler', self.context)
        b = p.custom('parser handler')

        self.assertIsInstance(b, Blob)
        self.assertEqual(b._sourceid, 'sourceid')
        self.assertEqual(b._label, 'label')
        self.assertIsInstance(b._panel, UploaderPanel)
        self.assertEqual(b._source, 'handler')

    def test_uploaderPanelShouldPushDataToStream(self):
        fm = FileMock()
        fm.write = MagicMock()

        parser = MagicMock(return_value="yolo")

        p = UploaderPanel(fm, parser)
        p.push(['data', 'to', 'write'])

        fm.write.assert_called_with('yolo\nyolo\nyolo')
        parser.assert_has_calls(list(map(call, ['data', 'to', 'write'])))

    def test_uploaderPanelShouldCloseTheFile(self):
        fm = FileMock()
        fm.close = MagicMock()
        parser = MagicMock()

        p = UploaderPanel(fm, parser)
        location = p.close()

        fm.close.assert_called_with()
        self.assertEqual(location, "file_location/file_name")

    def test_uploaderPanelShouldRemoveTheFile(self):
        fm = FileMock()
        fm.remove = MagicMock()
        parser = MagicMock()

        p = UploaderPanel(fm, parser)
        p.remove()

        fm.remove.assert_called_with()


    def test_blobShouldBuildAndUploadTheFile(self):
        source = MagicMock(side_effect=[ None, 0, 'text', True, False, 'not call' ])
        parser = MagicMock(return_value='row')
        tf = self.context.tempFile()
        tf.close = MagicMock()
        tf.remove = MagicMock()
        blobManager, url = self.context.blob()
        blobManager.create_blob_from_path = MagicMock()


        self.context.http.post.side_effect = [ResponseMock({
            "Data": {
                "return from": "createNewDataset",
                "some context": "goes here"
            }
        }), ResponseMock({
            "Data": {
                "some": "metadata"
            }
        })]

        blob = Blob('yolo', 'label', source, parser, self.context)
        ds = blob.upload()

        self.assertEqual(source.call_count, 5)
        tf.close.assert_called_with()
        blobManager.create_blob_from_path.assert_called_with('uploads', 'file_name', 'file_location/file_name')
        self.context.http.post.assert_has_calls([
            call('/root/v1/datasetimport/manageDatasetCreation', {
                "command":"createNewDataset",
                "context": {
                    "wizard": False,
                    "datasetid":"yolo",
                    "label":"label",
                    "provider":"RemoteFile",
                    "payload": {
                        "path": "base url/uploads/file_name"
                    }
                }
            })
        ])

        tf.remove.assert_called_with()

        self.assertIsInstance(ds, DatasourceInstance)
        self.assertEqual(ds._sourceid, 'yolo')

    def test_itExceptOnError(self):
        source = MagicMock(side_effect=[ None, 0, 'text', True, False, 'not call' ])
        parser = MagicMock(return_value='row')
        tf = self.context.tempFile()
        tf.close = MagicMock()
        tf.remove = MagicMock()
        blobManager, url = self.context.blob()
        blobManager.create_blob_from_path = MagicMock()

        self.context.http.post.side_effect = [ResponseMock({
            "Data": {
                "Code": -1,
                "Message": "Error message"
            }
        })]

        blob = Blob('yolo', 'label', source, parser, self.context)

        with self.assertRaises(StatwolfException):
            blob.upload()

        tf.remove.assert_called_with()
