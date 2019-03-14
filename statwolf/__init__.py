from statwolf.exceptions import *

from statwolf.services import *
from statwolf import services, http, tempfile

from azure.storage.blob import BlockBlobService
from itertools import islice

class Context:
    def __init__(self, config):
        if not "host" in config or not "username" in config or not "password" in config:
            raise InvalidConfigException()

        self.config = config

        tokens = config['host'].split('/')
        config['host'] = '/'.join(tokens[0:3])
        config['root'] = '/' + '/'.join(tokens[3:])

        self.loader = getattr
        self.http = http.create(self.config)
        self.openFile = open
        self._blockBlobService = BlockBlobService
        self.islice = islice

    def toDashboard(self, url):
        return self.config['root'] + url

    def blob(self):
        url = self.toDashboard('/v1/datasetimport/env')
        config = self.http.post(url).json()["Data"]

        return self._blockBlobService(connection_string=config["connectionString"]),  config["baseUrl"]

    def tempFile(self):
        return tempfile.TempFile.create()

def _internal_create(context, service):
    serviceModule = context.loader(services, service)

    return serviceModule.create(context)

def create(config, service):
    return _internal_create(Context(config.copy()), service)
