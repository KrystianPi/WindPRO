from sqlalchemy import create_engine
from .config import get_config
import pandas as pd

def select_forecast(station, purpose='predict'):
    
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()
    
    if purpose == 'predict':
        query = f"SELECT * FROM forecast_temp"
    if purpose == 'test':
        query = f"SELECT * FROM forecast WHERE Time >= DATE_SUB(NOW(), INTERVAL 7 DAY)"

    # Use Pandas to read data from the database into a DataFrame
    df = pd.read_sql(query, connection)

    return df

def select_measurments(station, purpose='test'):
    # I need measurments for two cases:
    # Check performance on previous week
    # Retrain the model (happening montlhy)

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    if purpose == 'retrain':
        query = f"SELECT * FROM measurments_{station}"
    if purpose == 'test':
        query = f"SELECT * FROM measurments_{station} WHERE Time >= DATE_SUB(NOW(), INTERVAL 7 DAY)"

    df = pd.read_sql(query, connection)

    return df
