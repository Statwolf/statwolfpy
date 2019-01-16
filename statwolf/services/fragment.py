from statwolf.services.baseservice import BaseService
from statwolf.services.datasource import create as createDatasource

class FragmentInstance:
    def __init__(self, fragmentId, context, datasource):
        self._context = context
        self._baseUrl = '/fragment/' + fragmentId
        self._params = { }
        self._internalFilterCounter = 0
        self._datasource = datasource

    def timeframe(self, dateFrom, dateTo):
        self._params["timeframe"] = {
            "dateFrom": dateFrom,
            "dateTo": dateTo
        }

        return self

    def metrics(self, metrics):
        self._params["metrics"] = metrics

        return self

    def groupBy(self, dimensions):
        self._params["dimensions"] = dimensions

        return self

    def pivotBy(self, dimensions):
        self._params["crosstab"] = dimensions

        return self

    def take(self, amount):
        self._params["take"] = str(amount)

        return self

    def addFilter(self, field, operator, value):
        if not 'filter' in self._params:
            self._params["filter"] = {}

        self._params["filter"]["field_" + str(self._internalFilterCounter)] = field
        self._params["filter"]["operator_" + str(self._internalFilterCounter)] = operator
        self._params["filter"]["value_" + str(self._internalFilterCounter)] = { "value": value, "noSuggestions": True }
        self._params["filter"]["selector_" + str(self._internalFilterCounter)] = 'AND'

        self._internalFilterCounter += 1

        return self

    def filters(self, filterList):
        self._params.pop("filter", None)
        self._internalFilterCounter = 0

        for f in filterList:
            self.addFilter(f[0], f[1], f[2])

    def params(self):
        response = self._discover()

        params = { key: response[0][key] for key in ["filter", "timeframe", "metrics", "dimensions", "take"] }
        params.update(self._params)

        return params

    def create(self):
        response = self._req("extend")

        return create(self._context).explore(response[0]["fragmentId"])

    def link(self):
        return self.create()._baseUrl;

    def data(self):
        response = self._req("query")

        return response[0]["data"]["data"]

    def currentDatasource(self):
        params = self._discover()[0]
        return self._datasource.explore(params["table"])

    def _req(self, mode):
        return self._context.http.post(self._baseUrl, { "mode": mode, "params": self._params }).json()

    def _discover(self):
        return self._context.http.get(self._baseUrl + '/discover').json()

class Fragment(BaseService):
    def __init__(self, context, datasourceFactory):
        super(Fragment, self).__init__(context)
        self._datasourceFactory = datasourceFactory

    def explore(self, fragmentId):
        return FragmentInstance(fragmentId, self._context, self._datasourceFactory(self._context))

def create(context):
    return Fragment(context, createDatasource)
