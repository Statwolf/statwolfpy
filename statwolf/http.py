import requests

class Http:
    def __init__(self, r, config):
        self._config = config

    def get(self, path):
        return self._request(requests.get, path, None)

    def post(self, path, data=None):
        return self._request(requests.post, path, data)

    def _request(self, method, path, data):
        args = {
            "headers": {
                "statwolf-auth": self._config["username"] + ":" + self._config["password"]
            }
        }

        if(data != None):
            args["json"] = data


        url = self._config["host"] + path

        return method(url, **args)


def create(config):
    return Http(requests, config)
