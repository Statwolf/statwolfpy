import statwolf
import json
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

localdir = os.path.dirname(os.path.abspath(__file__))

datasource = statwolf.create({
    "host": host,
    "username": username,
    "password": password
}, "datasource")

def up(c):
    c.push([ { "a": 1, "day": "2019-03-11" }, { "a": 2, "day": "2019-03-11" }, { "a": 3, "day": "2019-03-11" } ])

    return False

# upload json from custom location
ds = datasource.upload("sourceid_custom_source", "my label_source").source(up).json().upload()
print(ds.schema())

# upload json data from file
ds = datasource.upload("sourceid_file", "my label_file").file(localdir + '/test-data').text().upload()
print(ds.schema())
