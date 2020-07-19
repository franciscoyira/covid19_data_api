from urllib.request import urlopen
import urllib.error
import json
import ssl
import sqlite3
from datetime import datetime, timedelta

base_url = "https://api.covid19api.com/total/country/"

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

# Get yesterday date in format for URLs
yesterday = datetime.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d") + 'T00:00:00Z'

# Run loop of queries
# General case (for all the countries)
for i in range(len(countries_id)):
    # Country_ID is countries_id[i]
    # ID in Python is i    

    print(slugs[i])

    # Create URL 
    if last_updates[i] == 'None':
        request_url_confirmed = base_url + slugs[i] + '/status/confirmed?from=' + '2020-01-01T00:00:00Z' + '&to=' + yesterday_str
        print('case1')
    # This condition checks if there is new data available
    elif datetime.strptime(last_updates[0], '%Y-%m-%d') > yesterday:
        request_url_confirmed = base_url + slugs[i] + '/status/confirmed?from=' + last_updates[i] + 'T00:00:00Z' + '&to=' + yesterday_str
        print('case2')
    else:
        print('case3')
        continue

    # Create URL variations for recovered and deaths
    request_url_deaths = request_url_confirmed.replace('confirmed', 'deaths')
    request_url_recovered = request_url_confirmed.replace('confirmed', 'recovered')

    # Retrieve data using the request URLs
    # Confirmed
    print(request_url_confirmed)
    try:
        conn_confirmed = urlopen(request_url_confirmed, context=ctx)
        data_confirmed = conn_confirmed.read().decode()
    except:
        print('Failed to retrieve data from URL')
        continue

    js_confirmed = json.loads(data_confirmed)

    # Check remaining requests 
    headers = dict(conn_confirmed.getheaders())
    print('Requesting data. Remaining', headers['X-Ratelimit-Remaining'])

    # Process data and save it in lists
    dates = list()
    confirmed = list()

    for element in js_confirmed:
        dates.append(element['Date'][:10])
        confirmed.append(int(element['Cases']))

    # Deaths
    conn_deaths = urlopen(request_url_deaths, context=ctx)
    data_deaths = conn_deaths.read().decode()
    js_deaths = json.loads(data_deaths)

    deaths = list()

    for element in js_deaths:
        deaths.append(int(element['Cases']))

    # Recovered
    conn_recovered = urlopen(request_url_recovered, context=ctx)
    data_recovered = conn_recovered.read().decode()
    js_recovered = json.loads(data_recovered)

    recovered = list()

    for element in js_recovered:
        recovered.append(int(element['Cases']))

    # Create tuples to insert in DB
    #  (country_id, datetime, confirmed, recovered, deaths)

    to_insert = list()
    for j in range(len(dates)):
        t = (countries_id[i], dates[j], confirmed[j], recovered[j], deaths[j])
        to_insert.append(t)

    # Query to insert values
    insert_query = '''
    INSERT INTO Cases
    (country_id, datetime, confirmed, recovered, deaths)
    VALUES (?, ?, ?, ?, ?)'''

    cur.executemany(insert_query, to_insert)

    # Update 'last_updated' field in Countries table
    # Get today's date
    today_str = datetime.today().strftime("%Y-%m-%d") 

    cur.execute(
        '''UPDATE Countries
        SET last_updated = ?
        WHERE id = ?''',
        (today_str, countries_id[i]))

    conn.commit()
    
cur.execute('''
DELETE FROM New_Cases''')

cur.execute('''
INSERT INTO New_Cases 
SELECT
  Cases.*
  ,IFNULL((Cases.confirmed - LAG(Cases.confirmed) OVER(PARTITION BY country_id ORDER BY datetime)), 0) AS new_cases
  ,IFNULL((Cases.recovered - LAG(Cases.recovered) OVER(PARTITION BY country_id ORDER BY datetime)), 0) AS new_recovered
  ,IFNULL((Cases.deaths - LAG(Cases.deaths) OVER(PARTITION BY country_id ORDER BY datetime)), 0) AS new_deaths
FROM Cases
ORDER BY country_id, datetime;
''')

conn.commit()
cur.close()