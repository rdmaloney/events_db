import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import string
import re
import datetime
import sqlite3
import time
import os

links = []
alphabets = sorted(set(string.ascii_lowercase))

all_links = []
location = []
date = []
e_name = []
fights = []
f1 = []
f2 = []




def scrape_data():

        data = requests.get("http://ufcstats.com/statistics/events/upcoming?page=all")
        soup = BeautifulSoup(data.text, 'html.parser')
        table = soup.find('table', {"class": "b-statistics__table-events"})
        links = table.find_all('a', href=True)

        for link in links:
            all_links.append(link.get('href'))

        for link in all_links:
            print(f"Now currently scraping link: {link}")

            data = requests.get(link)
            soup = BeautifulSoup(data.text, 'html.parser')
            time.sleep(1)

            h2 = soup.find("h2")
            e_name.append(h2.text)

            box_item = soup.find("b-list__box-list-item")

            place = box_item[0].text
            d = box_item[1].text

            place.append(location)
            d.append(date)

            rows = soup.find_all('table', {"class": "b-fight-details__table b-fight-details__table_style_margin-top b-fight-details__table_type_event-details js-fight-table"})

            for row in rows:

                fights = row.find_all('td', {"class": "b-fight-details__table-col l-page_align_left"})

                fighters = row.find_all('p', {"class": "b-fight-details__table-text"})

                fighter1 = fighters[0].text
                fighter2 = fighters[1].text

                fighter1.append(f1)
                fighter2.append(f2)




        return None

#preprocessing
# remove rows where DOB is null
# impute stance as orthodox for missing stances
def create_df():
    #create empty dataframe
    df = pd.DataFrame()

    df["Event"] = e_name
    df["Date"] = date
    df["Location"] = location
    df["Fighter1"] = f1
    df["Fighter2"] = f2

    return df


def preprocessing(df):
    # identifying NaNs
    df = df.replace('--', np.nan)
    df = df.replace('', np.nan)

    return df


scrape_data()
df = create_df()
df = preprocessing(df)
print("Scraping completed")

conn = sqlite3.connect('data.sqlite')
df.to_sql('data', conn, if_exists='replace')
print('Db successfully constructed and saved')
conn.close()



