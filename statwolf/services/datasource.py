from statwolf.services.baseservice import BaseService
from statwolf import StatwolfException
from os.path import basename

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

class DatasourceInstance(BaseService):
    def __init__(self, sourceid, context):
        super(DatasourceInstance, self).__init__(context)

        self._baseUrl = context.toDashboard('/v1/full')
        self._sourceid = sourceid;
        self._meta = self.post(self._baseUrl + '/getSchema', {
            "sourceid": sourceid
        })

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

    def push(self, data):
        data = list(map(self._parser, data))
        self._file.write('\n'.join(data))

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
        while self._source(self._panel) is not False:
            pass
        location = self._panel.close()
        filename = basename(location)
        blob, baseUrl = self._context.blob()

        blob.create_blob_from_path('uploads', filename, location)
        blobUrl = baseUrl + 'uploads/' + filename

        commandPath = self._context.toDashboard('/v1/datasetimport/manageDatasetCreation')
        context = self.post(commandPath, {
            "command": "createNewDataset",
            "context": {
                "datasetid": self._sourceid,
                "label": self._label,
                "provider": "RemoteFile"
            }
        })

        context["payload"] = {
            "path": blobUrl
        }

        try:
            for stage in [ 'populateMetadata', 'prepareForUpload', 'completeUpload' ]:
                if 'Code' in context:
                    raise StatwolfException(context['Message'])

                context = self.post(commandPath, {
                    "command": stage,
                    "context": context
                })

            return DatasourceInstance(self._sourceid, self._context)
        finally:
            self._panel.remove()

class Parser(BaseService):
    def __init__(self, sourceid, label, source, context):
        super(Parser, self).__init__(context)
        self._sourceid = sourceid
        self._label = label
        self._source = source

    def custom(self, handler):
        return Blob(self._sourceid, self._label, self._source, handler, self._context)

class Upload(BaseService):
    def __init__(self, sourceid, label, context):
        super(Upload, self).__init__(context)
        self._sourceid = sourceid
        self._label = label

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

def create(context):
    return Datasource(context)
