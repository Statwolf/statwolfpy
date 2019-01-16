from unittest import TestCase
from unittest.mock import MagicMock

import statwolf.http as http
from statwolf.http import Http

import requests

class HttpTestCase(TestCase):

    def test_itShouldAllocateAnHttpService(self):
        client = http.create({ "host": "http://an.host", "username": "a username", "password": "a password" })
        self.assertIs(type(client), Http)

    def test_itShouldCompileAGetRequest(self):
        reply = {}
        requests.get = MagicMock(return_value=reply)

        config = { "host": "http://an.host", "username": "a username", "password": "a password" }

        client = Http(requests, config)
        response = client.get('/a path')

        headers = {
            "statwolf-auth": "a username:a password"
        }

        requests.get.assert_called_with('http://an.host/a path', headers=headers)
        self.assertEqual(reply, response);

    def test_itShouldCompileAnEmptyPostRequest(self):
        reply = {}
        requests.post = MagicMock(return_value=reply)

        config = { "host": "http://an.host", "username": "a username", "password": "a password" }

        client = Http(requests, config)
        response = client.post('/a path')

        headers = {
            "statwolf-auth": "a username:a password"
        }

        requests.post.assert_called_with('http://an.host/a path', headers=headers)
        self.assertEqual(reply, response);


    def test_itShouldCompileAPostRequest(self):
        reply = {}
        requests.post = MagicMock(return_value=reply)

        config = { "host": "http://an.host", "username": "a username", "password": "a password" }

        data = { "some": "data" }

        client = Http(requests, config)
        response = client.post('/a path', data)

        headers = {
            "statwolf-auth": "a username:a password"
        }

        requests.post.assert_called_with('http://an.host/a path', json=data, headers=headers)
        self.assertEqual(reply, response);
