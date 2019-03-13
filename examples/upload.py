import statwolf
import json
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

datasource = statwolf.create({
    "host": host,
    "username": username,
    "password": password
}, "datasource")

def up(c):
    c.push([ { "a": 1, "day": "2019-03-11" }, { "a": 2, "day": "2019-03-11" }, { "a": 3, "day": "2019-03-11" } ])

    return False

ds = u.upload("sourceid", "my label").source(up).custom(lambda x : json.dumps(x)).upload()
