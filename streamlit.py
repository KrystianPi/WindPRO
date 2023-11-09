import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import base64

######################################################## HELPER FUNCTIONS ########################################################
def get_config():
    PG_HOST=st.secrets.db_credentials.pg_host 
    PG_PORT=st.secrets.db_credentials.pg_port
    PG_DATABASE=st.secrets.db_credentials.pg_database
    PG_USER=st.secrets.db_credentials.pg_user
    PG_PASSWORD=st.secrets.db_credentials.pg_password

    # Create the MySQL database connection string
    db_url = f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}'
    
    return db_url

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

    # Define refined bins and labels for wind direction
    bins = np.arange(0, 361, 22.5)
    labels = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW', 'N']

    # Bin wind directions with the refined bins and labels
    df['Direction'] = pd.cut(df['Direction'] % 360, bins=bins, labels=labels[:-1], include_lowest=True)

    # Mapping for direction arrows
    arrows = {
        'N': '↓', 'NNE': '↓', 'NE': '↙', 'ENE': '←',
        'E': '←', 'ESE': '←', 'SE': '↗', 'SSE': '↗',
        'S': '↑', 'SSW': '↑', 'SW': '↖', 'WSW': '→',
        'W': '→', 'WNW': '→', 'NW': '↘', 'NNW': '↓'
    }

    # Add a new column for the arrows
    df['Arrow'] = df['Direction'].map(arrows)

    return df[['Day', 'Hour', 'Wind [kt]', 'Gust [kt]', 'Direction', 'Arrow']]

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
#############################################################################################################################################

if __name__ == '__main__': 
    set_background_image_with_base64('kuznica.jpeg')

    # Dropdown to select the table
    option = st.selectbox(
        'Which location would you like to display?',
        ('rewa', 'kuznica'),
        placeholder="Select location...",
        index=None,
    )

    db_url = get_config()

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Use the engine to connect to the database
    connection = engine.connect()

    query_rewa = 'SELECT * FROM current_pred_rewa'
    query_kuznica = 'SELECT * FROM current_pred_kuznica'

    df_rewa = pd.read_sql(query_rewa, connection)
    df_kuznica = pd.read_sql(query_kuznica, connection)

    connection.close()
    # Function to format the day with suffix

    def format_day_suffix(d):
        if 10 <= d <= 20:
            suffix = 'th'
        else:
            suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')
        return f"{d}{suffix}"

    # Transform the DataFrame
    transformed_df_rewa = transform_dataframe(df_rewa)
    transformed_df_kuznica = transform_dataframe(df_kuznica)


    if option == 'rewa':
        set_background_image_with_base64('rewa.jpeg')
        # Title for the table
        st.markdown("### Forecast for Rewa, Poland enhanced with Machine Learning")

        # Display the transformed DataFrame
        st.dataframe(transformed_df_rewa, width = 500, height = 800)
    elif option == 'kuznica':
        set_background_image_with_base64('kuznica.jpeg')
        st.markdown("### Forecast for Kuznica, Poland enhanced with Machine Learning")

        # Display the transformed DataFrame
        st.dataframe(transformed_df_kuznica, width = 500, height = 800)
