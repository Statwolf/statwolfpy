import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

service = statwolf.create({ "host": host, "username": username, "password": password }, "datasource")

# Load datasource
source = service.explore("uploaded_sourceid_file_mammeta_2_yolo")
pipeline = source.builder().timeframe('2000-01-01', '2019-05-01').dimensions(["a"]).sort([["a", "asc"]]).build()

def transformation(element, panel):
    element['meta']['pau'] = "new field"

    return element

pipeline = pipeline.transform(transformation)

print(pipeline.execute())
