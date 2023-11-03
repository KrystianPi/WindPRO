import streamlit as st
from sqlalchemy import create_engine
import pandas as pd


def get_config():
    PG_HOST=st.secrets.db_credentials.pg_host 
    PG_PORT=st.secrets.db_credentials.pg_port
    PG_DATABASE=st.secrets.db_credentials.pg_database
    PG_USER=st.secrets.db_credentials.pg_user
    PG_PASSWORD=st.secrets.db_credentials.pg_password

    # Create the MySQL database connection string
    db_url = f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}'
    
    return db_url

db_url = get_config()

# Create an SQLAlchemy engine
engine = create_engine(db_url)

# Use the engine to connect to the database
connection = engine.connect()

query = 'SELECT * FROM current_pred_rewa'

df = pd.read_sql(query, connection)

# # Color scale function
# def color_scale(val):
#     color = 'red' if val > 35 else 'orange' if val > 25 else 'green' if val > 10 else 'blue'
#     return f'background-color: {color}'

# # Apply the color scale to the temperature column
# styled_df = df.style.applymap(color_scale, subset=['temperature'])
st.dataframe(df)