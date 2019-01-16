import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

service = statwolf.create({ "host": host, "username": username, "password": password }, "datasource")

# Load datasource
source = service.explore("ga_xxxxxxx")

# Get datasource meta
print(source.schema)
print(source.dimensions())
print(source.metrics())
print(source.filters())

# Search for a single filter
print(source.filter('ga:deviceCategory'))

# Get values for a field
print(source.filters()[0].values())

# You can filter using a keyword
print(source.filters()[0].values("new"))

# Get the datasource information as plain json
print(source.raw())

# List datasources defined for the current dashboard
print(service.list())
