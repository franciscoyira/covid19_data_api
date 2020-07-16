from urllib.request import urlopen
import urllib.error
import json
import ssl
import sqlite3
from datetime import datetime, timedelta

base_url = "https://api.covid19api.com/country/"

# What this script will do:
# 1. Load the slugs for all the countries, and the date in which they were last updated
# 2. Iteratively query the API for each country, request data from the day after the last update, up to yesterday
# 3. Load that information in the database

# Connect to database
conn = sqlite3.connect('covid19.sqlite')
cur = conn.cursor()

cur.execute('''
            CREATE TABLE IF NOT EXISTS Cases
            (id INTEGER PRIMARY KEY, country_id INTEGER, datetime DATE, confirmed INTEGER, recovered INTEGER, deaths INTEGER)''')


# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Query country data (to be used in loop)
cur.execute('''
            SELECT id AS country_id, slug, last_updated
            FROM Countries''')

countries_id = list()
slugs = list()
last_updates = list()

for row in cur:
    countries_id.append(int(row[0]))
    slugs.append(str(row[1]))
    last_updates.append(str(row[2]))

# Run loop of queries

# Test case: Chile

# Get ID from Chile
cur.execute('''SELECT id 
FROM Countries
WHERE name = 'Chile' 
''')
py_id = cur.fetchone()[0] -1

# Get yesterday date in format for URL
yesterday = datetime.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d") + 'T00:00:00Z'


# Create URL 
if last_updates[py_id] is None:
    request_url = base_url + slugs[194] + '/status/confirmed?from=' + '2020-03-01T00:00:00Z' + '&to=' + yesterday_str
else:
    request_url = base_url + slugs[194] + '/status/confirmed?from=' + last_updates[py_id] + 'T00:00:00Z' + '&to=' + yesterday_str