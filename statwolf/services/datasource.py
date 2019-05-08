from statwolf.services.baseservice import BaseService
from statwolf import StatwolfException
from os.path import basename

import json
from dill.source import getsource
from textwrap import dedent
from copy import deepcopy

class Field:
    def __init__(self, sourceid, field, getHint):
        self._sourceid = sourceid;
        self._field = field;
        self._getHint = getHint;

    def name(self):
        return self._field;

    def values(self, hint=None):
        params = {
            "table": self._sourceid,
            "field": self._field,
        }

        if hint != None:
            params["text"] = hint

        return self._getHint(params)

    def __repr__(self):
        return self._field;

class Pipeline(BaseService):
    def __init__(self, query, pipeline, context):
        super(Pipeline, self).__init__(context)

        self._query = query
        self._pipeline = pipeline

    def query(self):
        return FluentQueryEditor(deepcopy(self._query), self._context)

    def execute(self, override=None):
        element = {
            "meta": {},
            "dataset": []
        }

        query = self._query

        if override != None:
            query = override._params

        for h in self._pipeline:
            panel = {
                'statwolf': self._context.http,
                'params': json.loads(h['params']),
                'query': query
            }
            exec(h['source'], {}, { 'element': element, 'panel': panel })
            element = panel['newElement']

        return element

class StepBuilder(BaseService):
    def __init__(self, baseUrl, query, context):
        super(StepBuilder, self).__init__(context)

        self._pipeline = []
        self._query = query

        def loader(element, panel):
            from statwolf import StatwolfException
            import pandas

            params = panel['params']
            res = panel['statwolf'].post(params['baseUrl'] + '/debugQuery', panel['query']).json()

            if res["Data"] == False:
                raise StatwolfException('Invalid request: maybe an authentication error. Please check that host, username and password are correct')

            res = res["Data"]

            if res.get('hasErrors', False) == True:
                raise StatwolfException(res['errorMessage'])

            return {
                "meta": {
                    "schema": res["meta"]
                },
                "dataset": pandas.DataFrame(res["data"])
            }

        self.transform(loader, {
            'baseUrl': baseUrl
        })

    def transform(self, handler, params={}):
        source = dedent(getsource(handler))
        source = source + 'panel["newElement"] = ' + handler.__name__ + '(element, panel)\n';

        self._pipeline.append({
            'source': source,
            'params': json.dumps(params)
        })

        return self

    def build(self):
        return Pipeline(self._query, self._pipeline, self._context)

class ModelBuilder:
    def __init__(self):
        self._status = {
            "model_type": "NOT DEFINED",
            "target_name": "NOT DEFINED",
            "feature_names": "NOT DEFINED"
        }

    def target(self, name):
        self._status["target_name"] = name

        return self

    def features(self, features):
        self._status["feature_names"] = features

        return self

    def type(self, name):
        self._status["model_type"] = name

        return self

    def build(self):
        return self._status

class FluentQueryEditor(BaseService):
    def __init__(self, params, context):
        super(FluentQueryEditor, self).__init__(context)

        self._params = params

    def timeframe(self, dateFrom, dateTo):
        self._params["timeframe"] = {
            "dateFrom": dateFrom,
            "dateTo": dateTo
        }

        return self

    def dimensions(self, dimensions):
        self._params["dimensions"] = dimensions

        return self

    def calculated(self, field, sql):
        self._params["testing"]["calculated"][field] = sql

        return self

    def customMetric(self, name, sql=None, operator=None, field=None):
        definition = {}

        if sql != None:
            definition = {
                "type": "sql",
                "sql": sql
            }
        elif operator != None:
            definition = {
                "operator": operator
            }
            if field != None:
                definition["field"] = field

        self._params["testing"]["metrics"][name] = definition

        return self

    def join(self, name, joinType, by, fields, table=None, sql=None):
        if sql == None:
            sql = 'select ' + ', '.join(fields + by)
            if table != None:
                sql = sql + ' from ' + table

        self._params["testing"]["calculated"][name] = {
            "join": "any left",
            "query": sql,
            "by": by,
            "fields": fields
        }

        return self

    def metrics(self, metrics):
        self._params["metrics"] = metrics

        return self

    def where(self, filters):
        index = 0

        final = {}
        for row in filters:
            final["field_" + str(index)] = row[0]
            final["operator_" + str(index)] = row[1]
            final["value_" + str(index)] = { "value": row[2], "noSuggestions": True }
            final["selector_" + str(index)] = 'AND'
            index += 1

        self._params["filter"] = final

        return self

    def sort(self, sorting):
        self._params["sort"] = sorting

        return self

    def take(self, take):
        self._params["take"] = take

        return self

    def model(self, name, factory=None, forceTraining=False):
        if name not in self._params["testing"]["metrics"] and factory == None:
            raise StatwolfException('You must define a factory for model ' + name)

        if factory != None:
            model = {
                "field": name
            }
            model.update(factory(ModelBuilder()))

            config = {
                "type": "ml",
                "store": True
            }
            config.update(model)

            self._params["testing"]["metrics"][name] = config

        self._params["testing"]["metrics"][name]["rebuild"] = forceTraining

        return self

class PipelineBuilder(FluentQueryEditor):
    def __init__(self, sourceid, baseUrl, context):
        super(PipelineBuilder, self).__init__({
            "table": sourceid,
            "filter": {},
            "timeframe": {
                "period":"last7days",
                "granularity":"day"
            },
            "granularity": "overall",
            "dimensions": [],
            "metrics": [],
            "sort": [],
            "take": "5000",
            "testing": {
                "calculated": {},
                "metrics": {}
            }
        }, context)

        self._baseUrl = baseUrl

    def update(self):
        reply = self.post(self._baseUrl + '/getDatasetInformation', {
            "table": self._params["table"]
        });

        updates = {};

        for field in reply.get("fields", []):
            updates[field["field"]] = field

        for key, value in self._params["testing"]["calculated"].items():
            updates[key] = {
                "data_type": "String",
                "field": key,
                "is_dimension": True,
                "is_filter": True,
                "is_visible": True,
                "type": "field",
                "definition": {
                    "inline": value
                }
            }

        for key, value in self._params["testing"]["metrics"].items():
            metric = {}

            if value.get("type", None) == "sql" or value.get("type", None) == "ml" or value.get("operator", None) != None:
                metric = value
            else:
                continue

            updates[key] = {
                "data_type": "Float64",
                "field": key,
                "is_dimension": False,
                "is_filter": False,
                "is_visible": True,
                "type": "metric",
                "definition": metric
            }

        reply = self.post(self._baseUrl + '/setDatasetInformation', {
            "table": self._params["table"],
            "payload": {
                "options": {},
                "fields": list(updates.values())
            }
        });

        return DatasourceInstance(self._params["table"], self._context)

    def steps(self):
        return StepBuilder(self._baseUrl, self._params, self._context)

class DatasourceInstance(BaseService):
    def __init__(self, sourceid, context):
        super(DatasourceInstance, self).__init__(context)

        self._baseUrl = context.toDashboard('/v1/full')
        self._sourceid = sourceid;
        self._meta = self.post(self._baseUrl + '/getSchema', {
            "sourceid": sourceid
        })

    def builder(self):
        return PipelineBuilder(self._sourceid, self._baseUrl, self._context)

    def schema(self):
        return self._meta.get("schema", {})

    def dimensions(self):
        return self._wrap(self._meta.get("dimensions", []))

    def metrics(self):
        return self._wrap(self._meta.get("metrics", []))

    def filters(self):
        return self._wrap(self._meta.get("filters", []))

    def filter(self, name):
        items = self._wrap([ f for f in self._meta.get("filters", []) if f == name ])
        return items[0] if len(items) == 1 else None

    def raw(self):
        return self._meta

    def _wrap(self, items):
        l = lambda payload: self.post(self._baseUrl + '/getHints', payload)
        return list(map(lambda i: Field(self._sourceid, i, l), items))

class UploaderPanel:
    def __init__(self, tmpFile, parser):
        self._file = tmpFile
        self._parser = parser
        self._hasData = False

    def push(self, data):
        data = list(map(self._parser, data))
        self._hasData = len(data) > 0
        self._file.write('\n'.join(data) + '\n')

    def hasData(self):
        ret = self._hasData
        self._hasData = False

        return ret

    def remove(self):
        self._file.remove()

    def close(self):
        self._file.close()

        return self._file.location()

class Blob(BaseService):
    def __init__(self, sourceid, label, source, parser, context):
        super(Blob, self).__init__(context)
        self._sourceid = sourceid
        self._label = label
        self._source = source
        self._panel = UploaderPanel(context.tempFile(), parser)

    def upload(self):
        watchdog = 0

        while self._source(self._panel) is not False:
            if self._panel.hasData() == True:
                watchdog = 0
            else:
                watchdog = watchdog + 1

            if watchdog == 1001:
                raise StatwolfException('Max empty data limit. Remember to return False from your source function when the end is reached.')

        location = self._panel.close()
        filename = basename(location)
        blob, baseUrl = self._context.blob()

        blob.create_blob_from_path('uploads', filename, location)
        blobUrl = baseUrl + 'uploads/' + filename

        commandPath = self._context.toDashboard('/v1/datasetimport/manageDatasetCreation')

        try:
            context = self.post(commandPath, {
                "command": "createNewDataset",
                "context": {
                    "wizard": False,
                    "datasetid": self._sourceid,
                    "label": self._label,
                    "provider": "RemoteFile",
                    "payload": {
                        "path": blobUrl
                    }
                }
            })

            if 'Code' in context:
                raise StatwolfException(context['Message'])

            return DatasourceInstance(self._sourceid, self._context)
        finally:
            self._panel.remove()

class Parser(BaseService):
    def __init__(self, sourceid, label, source, context):
        super(Parser, self).__init__(context)
        self._sourceid = sourceid
        self._label = label
        self._source = source

    def text(self):
        return self.custom(lambda x : x.rstrip('\n\r'))

    def json(self):
        return self.custom(lambda x : json.dumps(x))

    def custom(self, handler):
        return Blob(self._sourceid, self._label, self._source, handler, self._context)

class Upload(BaseService):
    def __init__(self, sourceid, label, context):
        super(Upload, self).__init__(context)
        self._sourceid = sourceid
        self._label = label

    def file(self, path):
        localFile = self._context.openFile(path, 'r')

        def loader(panel):
            batch = list(self._context.islice(localFile, 1000))
            if batch == []:
                localFile.close()
                return False
            else:
                panel.push(batch)

        return self.source(loader)

    def source(self, handler):
        return Parser(self._sourceid, self._label, handler, self._context)

class Datasource(BaseService):
    def __init__(self, context):
        super(Datasource, self).__init__(context)

    def list(self):
        return self.post(self._context.toDashboard('/v1/full/listSchemas'), {})

    def explore(self, sourceid):
        return DatasourceInstance(sourceid, self._context)

    def upload(self, sourceid, label):
        return Upload(sourceid, label, self._context)

    def delete(self, sourceid):
        self.post(self._context.toDashboard('/v1/datasetimport/manageDatasetCreation'), {
            "command":"deleteDataset",
            "context": {
                "datasetid": sourceid
            }
        })

        return self

def create(context):
    return Datasource(context)
