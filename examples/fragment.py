import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

fragment = statwolf.create({
    "host": host,
    "username": username,
    "password": password
}, "fragment")

f = fragment.explore("ded20ab239fe9945a91f70c23381003c")

# Get fragment data
print(f.data())

# Update fragment parameters
f.timeframe("2018-01-01", "2018-01-31").metrics(["ga:bounceRate"]).groupBy(["ga:deviceCategory"]).addFilter('ga:deviceCategory', '==', 'mobile').take(10)

print(f.params())
print(f.data())

# Override current filters
f.filters([
    [ 'ga:deviceCategory', '==', 'desktop' ]
])

print(f.params())

# Create a new fragment having the current configuration
newF = f.create()
print(newF.params())
print(newF.data())

# Get the link
print(newF.link())

# Get the datasource connected to the fragment
ds = newF.currentDatasource()
print(ds.schema())
