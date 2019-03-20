import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

sql = statwolf.create({
    "host": host,
    "username": username,
    "password": password
}, "sql")

print(sql.query('select 1'))
