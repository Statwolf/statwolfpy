from unittest import TestCase
from unittest.mock import MagicMock

from statwolf.services import fragment
from statwolf.services.fragment import Fragment, FragmentInstance

from statwolf.mocks import ContextMock, ResponseMock, DatasourceMock

class FragmentFactoryTestCase(TestCase):

    def test_itShouldCreateTheFragmentService(self):
        context = {}
        f = fragment.create(context)

        self.assertIs(type(f), Fragment)
        self.assertEqual(f._context, context)

class FragmentTestCase(TestCase):

    def test_itShouldCreateAFragmentInstance(self):
        context = ContextMock();

        instance = fragment.create(context).explore("a fragment id")

        self.assertIs(type(instance), FragmentInstance)
        self.assertEqual(instance._context, context)
        self.assertEqual(instance._baseUrl, "/fragment/a fragment id")

class FragmentInstanceTestCase(TestCase):

    def test_itShouldRetrieveTheFragmentData(self):
        context = ContextMock();

        data = [{ "data": { "data": [{ "item": 0 }] } }]
        response = ResponseMock(data)
        context.http.post = MagicMock(return_value=response)

        f = fragment.create(context).explore("an id")
        result = f.data()
        context.http.post.assert_called_with("/fragment/an id", { "mode": "query", "params": f._params })
        self.assertEqual(result, data[0]["data"]["data"])

    def test_itShouldUpdateTheParameters(self):
        context = ContextMock();

        data = [{ "data": { "data": [{ "item": 0 }] } }]
        response = ResponseMock(data)
        meta = ResponseMock([{"filter": {}, "timeframe": {}, "metrics": [], "dimensions": [], "take": 0}])
        context.http.post = MagicMock(return_value=response)
        context.http.get = MagicMock(return_value=meta)

        f = fragment.create(context).explore("an id")
        result = f.timeframe("2018-01-01", "2018-01-31").metrics(["ga:bounceRate"]).pivotBy(['ga:deviceCategory']).groupBy(["ga:deviceCategory"]).take(10).addFilter('a field', '==', 'pippo')
        self.assertEqual({
            'timeframe': { 'dateFrom' : '2018-01-01', 'dateTo': '2018-01-31' },
            'metrics': [ "ga:bounceRate" ],
            'dimensions': [ "ga:deviceCategory" ],
            'take': '10',
            'crosstab': [ 'ga:deviceCategory' ],
            'filter': { 'field_0': 'a field', 'operator_0': '==', 'value_0': { "noSuggestions": True, "value": "pippo" }, 'selector_0': 'AND' }
        }, f._params)
        self.assertEqual(f.params(), f._params)

    def test_itShouldOverrideTheFilters(self):
        context = ContextMock();

        f = fragment.create(context).explore("an id")
        f.filters([[ "a", "==", "b" ]])
        self.assertEqual({ 'field_0': 'a', 'operator_0': '==', 'value_0': { "noSuggestions": True, "value": "b" }, 'selector_0': 'AND' }, f._params['filter'])
        f.filters([[ "b", "==", "a" ]])
        self.assertEqual({ 'field_0': 'b', 'operator_0': '==', 'value_0': { "noSuggestions": True, "value": "a" }, 'selector_0': 'AND' }, f._params['filter'])


    def test_itShouldShowTheCurrentParameters(self):
        context = ContextMock()

        data = [{"table":"ga_132655703","filter":{"field_0":"ga:deviceCategory","operator_0":"==","value_0":{"noSuggestions":True,"value":"pippo"},"selector_0":"AND"},"timeframe":{"period":"last7days","granularity":"day"},"ctimeframe":{"period":"last7days","granularity":"day"},"granularity":"overall","dimensions":[],"metrics":[],"crosstab":[],"take":"5000","pivotDimension":"__none","params":{},"_dynamic":{}}]
        response = ResponseMock(data)
        context.http.get = MagicMock(return_value=response)

        f = fragment.create(context).explore("an id")
        self.assertEqual({
            "filter":{
                "field_0":"ga:deviceCategory",
                "operator_0":"==",
                "value_0": {
                    "noSuggestions": True,
                    "value":"pippo"
                },
                "selector_0":"AND"
            },
            "timeframe": {
                "period": "last7days",
                "granularity":"day"
            },
            "dimensions": [],
            "metrics": [],
            "take":"5000"
        }, f.params())

        context.http.get.assert_called_with("/fragment/an id/discover")

    def test_itShouldGenerateANewFragment(self):
        context = ContextMock()

        response = ResponseMock([{
            "fragmentId": "new id"
        }])
        context.http.post = MagicMock(return_value=response)

        f = fragment.create(context).explore("an id")
        self.assertEqual(f.metrics(["test:metric"]).create()._baseUrl, '/fragment/new id')

        context.http.post.assert_called_with("/fragment/an id", {
            "params": f._params,
            "mode": "extend"
        })

    def test_itShouldGetTheLinkOfTheCurrentFragment(self):
        context = ContextMock()

        response = ResponseMock([{
            "fragmentId": "new id"
        }])
        context.http.post = MagicMock(return_value=response)

        f = fragment.create(context).explore("an id")
        self.assertEqual(f.metrics(["test:metric"]).link(), '/fragment/new id')

        context.http.post.assert_called_with("/fragment/an id", {
            "params": f._params,
            "mode": "extend"
        })

    def test_itShouldGetTheCurrentDatasource(self):
        context = ContextMock()

        data = [{"table":"ga_132655703","filter":{"field_0":"ga:deviceCategory","operator_0":"==","value_0":{"noSuggestions":True,"value":"pippo"},"selector_0":"AND"},"timeframe":{"period":"last7days","granularity":"day"},"ctimeframe":{"period":"last7days","granularity":"day"},"granularity":"overall","dimensions":[],"metrics":[],"crosstab":[],"take":"5000","pivotDimension":"__none","params":{},"_dynamic":{}}]
        response = ResponseMock(data)
        context.http.get = MagicMock(return_value=response)

        ret = { "mock": "" }

        dsMock = DatasourceMock()
        dsMock.explore = MagicMock(return_value=ret)

        factory = MagicMock(return_value=dsMock)

        d = Fragment(context, factory).explore("an id").currentDatasource();

        factory.assert_called_with(context)
        dsMock.explore.assert_called_with("ga_132655703")
        self.assertEqual(d, ret)
