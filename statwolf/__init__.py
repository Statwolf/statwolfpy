from statwolf.exceptions import *

from statwolf.services import *
from statwolf import services, http

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

    def toDashboard(self, url):
        return self.config['root'] + url

def _internal_create(context, service):
    serviceModule = context.loader(services, service)

    return serviceModule.create(context)

def create(config, service):
    return _internal_create(Context(config.copy()), service)
