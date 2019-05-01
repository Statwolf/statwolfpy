import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

service = statwolf.create({ "host": host, "username": username, "password": password }, "datasource")

# Load datasource
builder = service.explore("uploaded_my_test_222").builder()

def linearRegression(b):
    return b.type('linear_regression').target('yolo').features([ 'myavg' ]).build()

ds = builder.calculated('yolo', 'formatDateTime(toDate(\'1987-04-22\'), \'%V\')')\
        .customMetric('myCount', operator="count")\
        .customMetric('myAvg', operator="avg", field="mySql")\
        .customMetric('mySql', sql='1')\
        .model('myModel', linearRegression)\
        .update()

# it returns the updated datasource
print(ds.schema())
