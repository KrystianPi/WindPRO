from config import get_config
from sqlalchemy import create_engine
import pandas as pd

def predict(station):
    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    # Specify the SQL query to retrieve data from a table
    query = f"SELECT * FROM forecast_{station}_temp"

    # Use Pandas to read data from the database into a DataFrame
    df = pd.read_sql(query, connection)