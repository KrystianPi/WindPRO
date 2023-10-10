from config import get_config
from sqlalchemy import create_engine
from train import Model
import pandas as pd

def transform(df):
    # Convert the 'Time' column to datetime
    df['Time'] = pd.to_datetime(df['Time'])

    # Set the 'Time' column as the index
    df.set_index('Time', inplace=True)

    # Resample the data with a two-hour interval and apply mean aggregation
    df = df.resample('2H').mean()
    
    return df

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

    df = transform(df)

    model = Model(station='rewa', model_path='model_store/model.pkl')

    X = df[model.feature_names]

    return model.predict(X)

if __name__ == '__main__': 
    print(predict(station='rewa'))



