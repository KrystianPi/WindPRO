from sqlalchemy import create_engine
from .measurments import get_measurments
from .forecast import get_forecast
from .config import get_config
from pathlib import Path
import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

def ingest_measurments(station, past_days):
    '''Get the measurments from wind station and ingest into db. Used only for monitoring and retraining'''     
    # Get data
    df = get_measurments(station, past_days)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Define table name based on the station
    table_name = f'measurements_{station}'

    # Query the most recent date in the measurements table
    query = f'SELECT MAX(Time) FROM {table_name}'
    last_date_in_db_result = engine.execute(query).fetchone()
    last_date_in_db = last_date_in_db_result[0] if last_date_in_db_result else None

    # Convert to date if not None
    if last_date_in_db:
        last_date_in_db = last_date_in_db.date()

    # Filter the dataframe for new measurements
    if last_date_in_db:
        df['Time'] = pd.to_datetime(df['Time']).dt.date
        df_new_measurements = df[df['Time'] > last_date_in_db]
    else:
        df_new_measurements = df

    # Check if there is new data to append
    if not df_new_measurements.empty:
        # Append new measurements to the database
        df_new_measurements.to_sql(table_name, engine, if_exists='append', index=False)
        print(f'New measurements for {station} ingested successfully into {table_name}.')
    else:
        print('No new measurements to ingest.')

def ingest_forecast():
    '''Get forecast for 3 days ahead and ingest into temp table in db. Used for inference'''
    # New forecast needs to be fed everyday because predictions for the current and next day will be made every day
    table_name = f'forecast_temp'
    
    # Get data
    df = get_forecast()

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Insert the Pandas DataFrame into the MySQL table
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Forecast for {df["Time"]} ingested successfully!')   
    except Exception as e:
        print(f"Data type mismatch or other data error: {e}")

def ingest_hist_forecast(past_days, forecast_days):
    '''Get forecast of the past week or more and ingest into forecast_weekly after one week ingest into main forecast table''' 
    '''Used for monitoring and retraining'''
    # Table containing all historical forecast

    # Get past week data
    df = get_forecast(past_days, forecast_days)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Fetch the last date in the forecast table
    last_date_query = 'SELECT MAX(Time) FROM forecast'
    with engine.connect() as conn:
        result = conn.execute(last_date_query)
        last_date_in_forecast = result.fetchone()[0]

    # If the table is empty, then assume we need all the data from the function call
    if last_date_in_forecast is None:
        last_date_in_forecast = datetime.datetime.strptime(df['Time'].min(), '%Y-%m-%d').date()
    else:
        last_date_in_forecast = datetime.datetime.strptime(last_date_in_forecast, '%Y-%m-%d').date()

    # Calculate the number of days between the last date in the forecast table and today
    delta_days = (datetime.date.today() - last_date_in_forecast).days

    # If there are days to update
    if delta_days > 0:
        # Filter the DataFrame for dates that are newer than the last date in the forecast table
        df_filtered = df[df['Time'] > last_date_in_forecast.strftime('%Y-%m-%d')]
        
        if not df_filtered.empty:
            table_name = 'forecast'
            df_filtered.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Appended new forecast data from {last_date_in_forecast + datetime.timedelta(days=1)} to {datetime.date.today()} to the main table.")
        else:
            print("No new dates to append to the forecast table.")
    else:
        print("The forecast table is up-to-date as of today.")

def ingest_predictions_temp(station, pred):
    '''Used for inference. Ingest predictions of the model to db so later streamlit can take from there'''
    table_name = f'current_pred_{station}'
    
    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    pred.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f'Prediction for {station} ingested successfully!')

def record_training(station, model_name):
    '''Records the last retraining of the model to the RDS postgres'''
    table_name = f'table_update_{station}'

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Create a DataFrame with the necessary data
    df = pd.DataFrame({
        'model_name': [model_name],
        'retrained_date': [datetime.datetime.now().date()]  # Gets today's date
    })

    # Append the data to the SQL table
    df.to_sql(table_name, engine, if_exists='append', index=False)
    
if __name__ == '__main__': 
    # ingest_measurments(14)
    ingest_hist_forecast()