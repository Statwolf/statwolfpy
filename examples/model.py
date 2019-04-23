import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

service = statwolf.create({ "host": host, "username": username, "password": password }, "datasource")

# Load datasource
source = service.explore("uploaded_my_test_222").builder()

def linearRegression(b):
    return b.type('linear_regression').target('yolo').features([ 'myavg' ]).build()

print(source.model('myLinearRegression', linearRegression).metrics(['myLinearRegression', 'myavg', 'yolo']).steps().build().execute())
