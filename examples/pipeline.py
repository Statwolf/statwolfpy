import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

service = statwolf.create({ "host": host, "username": username, "password": password }, "datasource")

# Load datasource
source = service.explore("uploaded_sourceid_file")

def transformation(element, panel):
    element['meta']['pau'] = "new field"

    return element

pipeline = source.builder()\
        .calculated('yolo', 'formatDateTime(toDate(\'1987-04-22\'), \'%V\')')\
        .customMetric('myCount', operator="count")\
        .customMetric('myAvg', operator="avg", field="myCount")\
        .customMetric('mySql', sql='1')\
        .join('my join', 'any left', ['yolo'], ['mytext'], sql='select formatDateTime(toDate(\'1987-04-22\'), \'%V\') as yolo, \'birthday\' as mytext')\
        .join('my join', 'any left', ['yolo'], ['mytext'], table='table.name')\
        .dimensions(['yolo', 'mytext'])\
        .metrics(['myCount'])\
        .steps()\
        .transform(transformation).build()

print(pipeline.execute())
