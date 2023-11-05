import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import base64

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

connection.close()
# Function to format the day with suffix

def format_day_suffix(d):
    if 10 <= d <= 20:
        suffix = 'th'
    else:
        suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')
    return f"{d}{suffix}"

# Function to apply the transformations
def transform_dataframe(df):
    # Convert the 'Time' column to datetime if it's not already
    df['Time'] = pd.to_datetime(df['Time'])

    # Extract the day and format it
    df['Day'] = df['Time'].apply(lambda x: x.strftime(f"%A {format_day_suffix(x.day)}"))

    # Extract the hour
    df['Hour'] = df['Time'].dt.hour

    # Round the 'Wind' column to 0 decimal places
    df['Wind [kt]'] = df['Wind'].round(0).astype(int)

    # Round the 'Wind' column to 0 decimal places
    df['Gust [kt]'] = df['Gust'].round(0).astype(int)

    # Drop the original 'Time' column
    df = df.drop(columns=['Time'])

    bins = [0, 45, 90, 135, 180, 225, 270, 315, 360]
    labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

    # Bin wind directions 
    df['Direction'] = pd.cut(df['Direction'], bins=bins, labels=labels)

    return df[['Day', 'Hour', 'Wind [kt]', 'Gust [kt]', 'Direction']]

# Transform the DataFrame
transformed_df = transform_dataframe(df)

def get_base64_of_file(file_path):
    with open(file_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

def set_background_image_with_base64(file_path):
    base64_image = get_base64_of_file(file_path)
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{base64_image}");
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

set_background_image_with_base64('rewa.jpeg')

# Title for the table
st.markdown("### Forecast for Rewa, Poland enhanced with Machine Learning")

# Display the transformed DataFrame
st.dataframe(transformed_df, width = 300, height = 800)
