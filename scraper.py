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

        table = soup.find('table', {'width': "98%"})
        links = table.find_all('a', href=True)

        for link in links:
            all_links.append("http://ufcstats.com/"+link.get('href'))

        for link in all_links:
            print(f"Now currently scraping link: {link}")

            data = requests.get(link)
            soup = BeautifulSoup(data.text, 'html.parser')
            time.sleep(1)

            rows = soup.find_all('table', {'cellspacing': "5"})

            h2 = soup.find("h2")

            e_name.appen(h2.text)


            for row in rows:

                f1 = soup. find_all ('table', { "class" : "b-fight-details__table-text" [0]})
                

                f2 = soup.find_all('table', {"class": "b-fight-details__table-text"[1]})
                

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



