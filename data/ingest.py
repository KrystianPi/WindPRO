from sqlalchemy import create_engine
from .measurments import get_measurments
from .forecast import get_forecast
from .config import get_config
from pathlib import Path
import datetime

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

def ingest_measurments(station, past_days):
    # Measurments are used only to assess performance and retrain model every month.     
    # Get data
    df = get_measurments(station, past_days)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Write to the file (this will replace contents or create a new file)
    filename = BASE_DIR / 'data' / 'logs' / f'last_ingest_measurments_{station}.txt'
    
    with open(filename, 'r') as f:
        last_date = f.read().strip()
    last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d').date()
    
    print((datetime.date.today() - last_date).days)
    
    # Weekly update, prevent duplicates
    if (datetime.date.today() - last_date).days < 7:
        table_name = f'measurments_{station}_weekly'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Weekly measurments for {df["Time"]} ingested successfully!') 
    elif (datetime.date.today() - last_date).days >= 7:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        with open(filename, 'w') as f:
            f.write(today)
        table_name = f'measurments_{station}'
        df.to_sql(table_name, engine, if_exists='append', index=False)  
        print(f'Weekly measurments for {df["Time"]} ingested successfully and appended to main table!')   

def ingest_forecast():
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

def ingest_hist_forecast(past_days):
    # Table containing all historical forecast

    # Get past week data
    df = get_forecast(past_days)

    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Write to the file (this will replace contents or create a new file)
    filename = BASE_DIR / 'data' / 'logs' / f'last_ingest_forecast.txt'
    
    with open(filename, 'r') as f:
        last_date = f.read().strip()
    last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d').date()
    
    print((datetime.date.today() - last_date).days)
    
    # Weekly update, prevent duplicates
    if (datetime.date.today() - last_date).days < 7:
        table_name = f'forecast_weekly'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Weekly forecast for {df["Time"]} ingested successfully!') 
    elif (datetime.date.today() - last_date).days >= 7:
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        with open(filename, 'w') as f:
            f.write(today)
        table_name = f'forecast'
        df.to_sql(table_name, engine, if_exists='append', index=False)  
        print(f'Weekly forecast for {df["Time"]} ingested successfully and appended to main table!') 

def ingest_predictions_temp(station, pred):
    table_name = f'current_pred_{station}'
    
    # Get database url
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    pred.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f'Prediction for {station} ingested successfully!')

if __name__ == '__main__': 
    # ingest_measurments(14)
    ingest_hist_forecast()