from sqlalchemy import create_engine
from measurments import get_measurments
from forecast import get_forecast
from config import get_config
import pandas as pd

def ingest_measurments(past_days):
    # Measurments are used only to assess performance and retrain model every month. 
    # When called it needs to feed every measurments from past month
    table_name = 'measurments_rewa'
    
    # Get data
    df = get_measurments(past_days)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Insert the Pandas DataFrame into the MySQL table this will be later joined with predictions and used to measure performance 
    df.to_sql(table_name, engine, if_exists='append', index=False)

def ingest_forecast():
    # New forecast needs to be fed everyday because predictions for the current and next day will be made every day
    table_name = 'forecast_rewa_temp'
    
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

def ingest_hist_forecast():
    # Table containing all historical forecast
    table_name = 'forecast_rewa'
    
    # Get past week data
    df = get_forecast(past=True)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Insert the Pandas DataFrame into the MySQL table
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f'Forecast for {df["Time"]} ingested successfully!')   
    except Exception as e:
        print(f"Data type mismatch or other data error: {e}")

def clean_duplicates(station):
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()
    
    query = f"SELECT * FROM forecast_{station}"

    df = pd.read_sql(query, connection)

    df.drop_duplicates(subset='Time', inplace=True)

    df.to_sql(f'forecast_{station}', engine, if_exists='replace', index=False)

if __name__ == '__main__': 
    # ingest_measurments(14)
    ingest_hist_forecast()
    # clean_duplicates('rewa')