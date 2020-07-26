import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.dates as mdate

# Retrieve historic data of the 5 countries with more deaths

# Connect to database
conn = sqlite3.connect('covid19.sqlite')
cur = conn.cursor()


df = pd.read_sql_query('''
SELECT
  Countries.name
  ,new_cases.datetime
  ,new_cases.new_cases
  ,new_cases.new_deaths
 FROM New_Cases
 INNER JOIN (
   SELECT
     country_id
	 ,SUM(new_deaths) AS total_deaths
   FROM New_Cases
   WHERE country_id != (
       SELECT id
       FROM Countries
       WHERE name = "United Kingdom"
   )
   GROUP BY 1
   ORDER BY total_deaths DESC
   LIMIT 5
 ) AS top_5_countries
   ON (top_5_countries.country_id = New_Cases.country_id)
 LEFT JOIN Countries
   ON (Countries.id = New_Cases.country_id)''', conn)

# Get top countries
top_countries = df.name.unique()
country_1 = df[df.name == top_countries[0]]
country_2 = df[df.name == top_countries[1]]
country_3 = df[df.name == top_countries[2]]
country_4 = df[df.name == top_countries[3]]
country_5 = df[df.name == top_countries[4]]

# Plot the series of new cases

# Set the locator
locator = mdate.MonthLocator(country_1.datetime)  # every month
# Specify the format - %b gives us Jan, Feb...
fmt = mdate.DateFormatter('%b')

plt.plot_date(country_1.datetime, country_1.new_deaths, '-')
plt.plot_date(country_2.datetime, country_2.new_deaths, '-')
plt.plot_date(country_3.datetime, country_3.new_deaths, '-')
plt.plot_date(country_4.datetime, country_4.new_deaths, '-')
plt.plot_date(country_5.datetime, country_5.new_deaths, '-')
plt.legend(top_countries)
plt.xlabel('date')
plt.ylabel('new deaths')

X = plt.gca().xaxis
X.set_major_locator(mdate.MonthLocator(country_1.datetime))
# Specify formatter
X.set_major_formatter(fmt)

plt.show()
