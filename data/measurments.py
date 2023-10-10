import requests
from bs4 import BeautifulSoup
import re
import numpy as np
import ast
import pandas as pd
from datetime import datetime, timedelta
import time

def get_measurments(start_date):
    df = pd.DataFrame()
    day_list = []
    while start_date <= datetime.today():
        day_list.append(start_date)
        start_date += timedelta(days=1)

    for date in day_list:
        # Extract info for url
        day = date.day
        month = date.month
        year = date.year

        url = f"http://www.wiatrkadyny.pl/rewa/wxwugraphs/graphd1a.php?theme=pepper&d={day}&m={month}&y={year}&w=900&h=350"

        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Regex to retrive data from html
            patternWindSpeed = r'var dAvgWS\s*=\s*(\[.*?\]);'
            patternWindGust = r'var dGustWS\s*=\s*(\[.*?\]);'
            patternTemp = r'var dTemp\s*=\s*(\[.*?\]);'
            patternWindDir = r'var dWindDir\s*=\s*(\[.*?\]);'
            patternBaro = r'var dBaro\s*=\s*(\[.*?\]);'

            # Use re.findall to find all matches in the HTML content
            WindSpeed = re.findall(patternWindSpeed, str(soup.contents), re.IGNORECASE | re.DOTALL)
            WindGust = re.findall(patternWindGust, str(soup.contents), re.IGNORECASE | re.DOTALL)
            Temp = re.findall(patternTemp, str(soup.contents), re.IGNORECASE | re.DOTALL)
            WindDir = re.findall(patternWindDir, str(soup.contents), re.IGNORECASE | re.DOTALL)
            Baro = re.findall(patternBaro, str(soup.contents), re.IGNORECASE | re.DOTALL)

            # Remove the trailing comma to make it a valid JSON-like format
            WindSpeed = ast.literal_eval(WindSpeed[0].rstrip(','))[:-1]
            WindGust = ast.literal_eval(WindGust[0].rstrip(','))[:-1]
            Temp = ast.literal_eval(Temp[0].rstrip(','))[:-1]
            WindDir = ast.literal_eval(WindDir[0].rstrip(','))[:-1]
            Baro = ast.literal_eval(Baro[0].rstrip(','))[:-1]

            # Convert from m/s to knots
            WindSpeed = [np.round(speed * 1.94384449,2) for speed in WindSpeed]
            WindGust = [np.round(speed * 1.94384449,2) for speed in WindGust]

            # Define the date and time components
            hour = 0  # Starting hour
            minute = 0  # Starting minute
            second = 0  # Starting second
            datetime_values = [] # Create a list to store datetime values
            # Create datetime values with 10-minute intervals for the entire day
            while hour < 24:
                current_datetime = datetime(year, month, day, hour, minute, second)
                datetime_values.append(current_datetime)
                minute += 10
                if minute == 60:
                    minute = 0
                    hour += 1

            # Create a DataFrame
            data = {
                'Time': datetime_values,
                'WindSpeed': WindSpeed,
                'WindGust': WindGust,
                'Temp': Temp,
                'WindDir': WindDir,
                'Baro': Baro
            }

            # Concat to the global dataframe, skip if data corrupted for current day
            try:
                df_i = pd.DataFrame(data)
                df = pd.concat([df,df_i])
            except ValueError:
                print(f'Data inconsistent for day: {day}.{month}.{year}. Skiping...')
                pass
            time.sleep(3)

        else:
            print(f"Failed to fetch data from {url}")

    return df