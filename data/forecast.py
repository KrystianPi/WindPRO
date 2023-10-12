import requests
import pandas as pd
from datetime import date

URL = "https://api.open-meteo.com/v1/gfs"
LATIUDE = 54.76
LONGITUDE = 18.51

def get_forecast(past = False):
    today = date.today()
    if past:
        forecast_days = 1
        past_days = 7
    else:
        forecast_days = 3
        past_days = 0

    # Define the parameters for the API request
    params = {
        "latitude": LATIUDE,
        "longitude": LONGITUDE,
        "hourly": "windspeed_10m,windgusts_10m,winddirection_10m",
        "windspeed_unit": "kn",
        "timezone": "Europe/Berlin",
        "forecast_days": forecast_days,
        "past_days": past_days
    }
    # Make the GET request to the API
    response = requests.get(URL, params=params)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        df = pd.DataFrame()
        # Access the specific data you need (e.g., windspeed, wind direction, and wind gusts)
        df['Time'] = pd.to_datetime(data['hourly']['time'])
        df['Month'] = df['Time'].dt.month
        df['Hour'] = df['Time'].dt.hour
        df['WindForecast'] = data["hourly"]["windspeed_10m"]
        df['GustForecast'] = data["hourly"]["winddirection_10m"]
        df['WindDirForecast'] = data["hourly"]["windgusts_10m"]

    else:
        # Handle the error
        print(f"Failed to retrieve data. Status code: {response.status_code}")
    if past:
        df = df[df['Time'].dt.date < today]
    return df

if __name__ == "__main__":
    get_forecast(past=True)