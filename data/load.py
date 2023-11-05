from sqlalchemy import create_engine
from .config import get_config
import pandas as pd

def select_forecast(station, purpose='predict'):
    '''Get forecast for predictions or for monitoring.'''
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()
    
    if purpose == 'predict':
        query = f"SELECT * FROM forecast_temp"
    if purpose == 'test':
        query = 'SELECT * FROM forecast WHERE "Time" >= NOW() - INTERVAL \'7 days\';'
    if purpose == 'retrain':
        query = f"SELECT * FROM forecast"

    # Use Pandas to read data from the database into a DataFrame
    df = pd.read_sql(query, connection)

    return df

def select_measurments(station, purpose='test'):
    '''Get measurments for monitoring or retraining'''

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    if purpose == 'retrain':
        query = f"SELECT * FROM measurments_{station}"
    if purpose == 'test':
        query = f'SELECT * FROM measurments_{station} WHERE "Time" >= NOW() - INTERVAL \'7 days\';'

    df = pd.read_sql(query, connection)

    return df
