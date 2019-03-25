from unittest import TestCase
from unittest.mock import MagicMock, call

from statwolf.services import datasource
from statwolf.services.datasource import Datasource, DatasourceInstance, Upload, Parser, Blob, UploaderPanel, PipelineBuilder, Pipeline
from statwolf import StatwolfException

from statwolf.mocks import ContextMock, ResponseMock, FileMock

import json

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

    def test_jsonParserShouldJustSerializeTheJson(self):
        p = Parser("source", 'label', 'handler', self.context)
        b = p.json()
        d = { 'a': 'test', 'json': 99 }

        self.assertEqual(json.dumps(d), b._panel._parser(d))

    def test_fileSourceShouldReadAFileByBatch(self):
        fm = FileMock()
        fm.close = MagicMock()

        panel = UploaderPanel('yolo', 'yolo')
        panel.push = MagicMock()

        self.context.openFile = MagicMock(return_value=fm)
        self.context.islice = MagicMock(side_effect=[
            [ 'a', 'b', 'c' ],
            []
        ])

        p = Upload('sourceid', 'label', self.context).file('local path')

        self.assertEqual(p._source(panel), None)
        self.assertEqual(p._source(panel), False)

        self.context.openFile.assert_called_with('local path', 'r')
        self.context.islice.assert_has_calls([
            call(fm, 1000),
            call(fm, 1000)
        ])
        panel.push.assert_called_with([ 'a', 'b', 'c' ])
        fm.close.assert_called_with()

    def test_textParserShouldJustRemoveTrailingLineTerminators(self):
        text = Parser("source", 'label', 'handler', self.context).text()

        self.assertEqual('  yolo', text._panel._parser('  yolo'))
        self.assertEqual('  yolo', text._panel._parser('  yolo\n'))
        self.assertEqual('  yolo', text._panel._parser('  yolo\r\n'))

    def test_datasourceInstanceShouldAllocateAQueryBuilder(self):
        d = DatasourceInstance('sourceid', self.context)
        q = d.builder()

        self.assertIsInstance(q, PipelineBuilder)
        self.assertEqual(q._baseUrl, d._baseUrl)
        self.assertEqual(q._context, d._context)
        self.assertEqual(q._params, {
            "table": 'sourceid',
            "filter": {},
            "timeframe": {
                "period":"last7days",
                "granularity":"day"
            },
            "granularity": "overall",
            "dimensions": [],
            "metrics": [],
            "sort": [],
            "take": "5000"
        })

    def test_datasourceInstanceShouldBuildAQueryPipeline(self):
        pb = PipelineBuilder('sourceid', 'the url', self.context)
        pipeline = pb.build()

        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(pipeline._context, self.context)

    def test_pipelineBuilderShuoldUpdateLoaderParameters(self):
        pb = PipelineBuilder('sourceid', 'the url', self.context)

        pb.timeframe('2019-01-01', '2019-02-02').dimensions(['yolo', 'yolo2']).metrics(['yolo3', 'yolo4']).where([
            [ 'pippolo', '==', 'plutolo' ],
            [ 'yolo', '>', 100 ]
        ]).sort([
            [ 'country', 'asc' ]
        ]).take("100")

        self.assertEqual(pb._params, {
            "table": 'sourceid',
            "filter": {
                "field_0": "pippolo",
                "operator_0": "==",
                "value_0": { "value": "plutolo", "noSuggestions": True },
                "selector_0": "AND",
                "field_1": "yolo",
                "operator_1": ">",
                "value_1": { "value": 100, "noSuggestions": True },
                "selector_1": "AND"
            },
            "timeframe": {
                "dateFrom": "2019-01-01",
                "dateTo": "2019-02-02"
            },
            "granularity": "overall",
            "dimensions": [ 'yolo', 'yolo2' ],
            "metrics": [ 'yolo3', 'yolo4' ],
            "sort": [ [ 'country', 'asc' ] ],
            "take": "100"
        })

    def test_pipelineShouldRunThePipelineAndGetTheResult(self):
        reply = ResponseMock({
            "Success": True,
            "Data": {
                "data": {
                    "meta": [{
                        "name": "a",
                        "type": "String",
                        "internalName": "a"
                    }, {
                        "name": "number_of_rows",
                        "type": "UInt64",
                        "internalName": "number_of_rows"
                    }],
                    "data": [{
                        "a": "3",
                        "number_of_rows": 1
                    }],
                    "totals": None,
                    "rows": 1,
                    "rows_before_limit_at_least": 3,
                    "hasErrors": False,
                    "errorMessage": None,
                    "display": "overall"
                    },
                "hints": {}
            }
        })
        self.context.http.post = MagicMock(return_value=reply)

        params = {
            "my params": "my value"
        }

        p = Pipeline('base url', params, self.context)

        def transform(element, panel):
            return {
                "dataset": 'the dataset',
                "meta": 'my meta',
                "element": element
            }

        p.transform(transform)

        self.assertEqual(p.execute(), {
            "dataset": 'the dataset',
            "meta": "my meta",
            "element": {
                "dataset": [{
                    "a": "3",
                    "number_of_rows": 1
                }],
                "meta": {
                    "schema": [{
                        "name": "a",
                        "type": "String",
                        "internalName": "a"
                    }, {
                        "name": "number_of_rows",
                        "type": "UInt64",
                        "internalName": "number_of_rows"
                    }]
                }
            }
        })

        self.context.http.post.assert_called_with('base url/$$base', params)

    def test_itShouldDeleteAdatasource(self):
        self.context.http.post = MagicMock()

        d = datasource.create(self.context)
        newD = d.delete('a source id')

        self.assertEqual(d, newD)
        self.context.http.post.assert_called_with('/root/v1/datasetimport/manageDatasetCreation', {
            "command":"deleteDataset",
            "context": {
                "datasetid": "a source id"
            }
        })

