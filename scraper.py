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
    for alpha in alphabets:
        links.append("http://ufcstats.com/statistics/events/upcoming" + alpha + "&page=all")

        # now that we have a list of links we need to iterate it with BeautifulSoup
    for link in links:
        print(f"Currently on this link: {link}")

        data = requests.get(link)
        soup = BeautifulSoup(data.text, 'html.parser')
        names = soup.find_all('a', {'class': 'b-link b-link_style_black'}, href=True)



        # list to store url page of events
        events = []

        for name in names:
            events.append(name['href'])

        events = sorted(set(events))

        for event in events:
            data = requests.get(event)
            soup = BeautifulSoup(data.text, 'html.parser')
            time.sleep(2)

            #event name
            n = soup.find('span', {'class': 'b-content__title-highlight'})
            e_name.append(n.text.strip())
            print(f"Scraping the following event: {n.text.strip()}")

            # event info box
            event_info = soup.find ('ul', {'class': 'b-list_box-list'})
            event_info = event_info.find_all('li')

            #event info- Date
            date = event_info[0].text.strip().strip("Date:").strip()

            # event info- Location
            location = event_info[1].text.strip().strip("Location:").strip()

            fights = soup.find('span', {'class': 'b-fight-details_table-col l-page_align_left'})

            f1 = soup.find('span', {'class': 'b-fight-details_table-text'[0]})

            f2 = soup.find('span', {'class': 'b-fight-details_table-text'[1]})

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
    df["Figter1"] = f1
    df["Figter2"] = f2

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


