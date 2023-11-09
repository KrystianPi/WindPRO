from sqlalchemy import create_engine
from .config import get_config
import pandas as pd
import datetime

def select_forecast(station, past_days=0, purpose='predict'):
    '''Get forecast for predictions or for monitoring.'''
    start_date = (datetime.datetime.now() - datetime.timedelta(days=past_days)).date()

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()
    
    if purpose == 'predict':
        query = f"SELECT * FROM forecast_temp"
    if purpose == 'test':
        query = f'''
                SELECT * FROM forecast
                WHERE "Time" >= '{start_date}';
                '''
    if purpose == 'retrain':
        query = f"SELECT * FROM forecast"

    # Use Pandas to read data from the database into a DataFrame
    df = pd.read_sql(query, connection)

    return df

def select_measurments(station, past_days=0, purpose='test'):
    '''Get measurments for monitoring or retraining'''
    start_date = (datetime.datetime.now() - datetime.timedelta(days=past_days)).date()

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    if purpose == 'retrain':
        query = f"SELECT * FROM measurments_{station}"
    if purpose == 'test':
            query = f'''
                    SELECT * FROM measurments_{station}
                    WHERE "Time" >= '{start_date}';
                    '''

    df = pd.read_sql(query, connection)

    return df

def select_training_date(station, model_name):
    '''Get the date of the last model training.'''
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    query = f"""
        SELECT retrained_date FROM table_update_{station}
        WHERE model_name = '{model_name}'
        ORDER BY retrained_date DESC
        LIMIT 1;
        """

    df = pd.read_sql(query, connection)

    last_date = df.iloc[0]['retrained_date']

    from datetime import date
    if isinstance(last_date, date):
        last_date = pd.to_datetime(last_date)

    connection.close()

    return last_date