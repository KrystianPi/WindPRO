import json
from sqlalchemy import create_engine
from measurments import get_measurments
from forecast import get_forecast
from config import get_config

def ingest_measurments(start_date):
    # Measurments are used only to assess performance and retrain model every month. 
    # When called it needs to feed every measurments from past month
    table_name = 'measurments_rewa'
    
    # Get data
    df = get_measurments(start_date=start_date)

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
    df.to_sql(table_name, engine, if_exists='replace', index=False)    
