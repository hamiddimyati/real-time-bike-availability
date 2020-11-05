import requests
import sys
from datetime import datetime
import pytz

year, month, day = sys.argv[1].split('-')
hour, minute = sys.argv[2].split(':')

url = 'http://localhost:8080/available_bikes'
myobj = {'latitude': '40.719258',
		 'longitude': '-74.002749',
		 'year': year,
		 'month': month,
		 'day': day,
		 'hour': hour,
		 'min': minute}

x = requests.post(url, data=myobj)

print(x.text)
