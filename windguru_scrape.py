#!/usr/bin/env python
import os

# Replace '/usr/local/bin/bin' with the actual path to your PhantomJS executable directory
webdriver_path = '/usr/local/bin/chromedriver_mac64'

# Get the current PATH and append the PhantomJS directory
os.environ['PATH'] = f"{webdriver_path}:{os.environ['PATH']}"

import re
from urllib.parse import urlparse

from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep

link = "https://www.windguru.cz/500757"

class Scraper(object):
    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.set_window_size(1120, 550)

    def scrape(self):
        print('Loading...')
        self.driver.get(link)

        forecast = {}

    # while True:
        s = BeautifulSoup(self.driver.page_source, "html.parser")
        text_file = open("forecast.txt", "w")
        text_file.write(s.find_all('script'))      
        text_file.close()
        rows = s.find("table", {"class": "tabulka"}).find("tbody").find_all("tr")
        # rows = s.find("table", {"class": "tabulka"}).find("tbody").find_all("tr", {"id": "tabid_0_0_WINDSPD"})

        for row in rows:
            cells = row.find_all("td")
            id = row['id']
            forecast[id] = []
            i = 0
            for cell in cells:
                if ('DIRPW' in id): # or ('DIRPW' in id):
                    print(id + " " + str(i))
                    value = cell.find('span').find('svg').find('g')["transform"]
                else:
                    value = cell.get_text()
                forecast[id].append(value)
                i = i + 1

        print(forecast)

        self.driver.quit()
        return forecast

if __name__ == '__main__':
    scraper = Scraper()
    scraper.scrape()