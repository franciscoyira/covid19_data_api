from urllib.request import urlopen
import urllib.error
import json
import ssl
import sqlite3

countries_url = "https://api.covid19api.com/countries"

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Make connection to API and load data
connection = urlopen(countries_url, context=ctx)
headers_countries = dict(connection.getheaders())

data_countries = connection.read().decode()
countries_json = json.loads(data_countries)

# Create database
conn = sqlite3.connect('covid19.sqlite')
cur = conn.cursor()

cur.execute('''
            CREATE TABLE IF NOT EXISTS Countries
            (name TEXT, slug TEXT, iso2 TEXT)''')

insert_query = '''
INSERT INTO Countries
(name, slug, iso2)
VALUES (?, ?, ?)'''

# Insert values
# Convert into tuples
values_tuples = [tuple(v) for v in map(dict.values, countries_json)]

# Insert the tuples into table
cur.executemany(insert_query, values_tuples)

conn.commit()
cur.close()
