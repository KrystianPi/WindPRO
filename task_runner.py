import requests
import sys
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd

# Load .env file
load_dotenv()

# Get the EC2 tracking server host from the environment variable
EC2_TRACKING_SERVER_HOST = os.getenv('EC2_TRACKING_SERVER_HOST')
EC2_ENDPOINT = f"http://{EC2_TRACKING_SERVER_HOST}:8000"

# Parameters for the RDS PostgreSQL instance
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Create the MySQL database connection string
db_url = f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}'

engine = create_engine(db_url)

connection = engine.connect()

query = 'select * from model_versions'

df = pd.read_sql(query, connection)

max_version = df['version'].max()

PARAMS = {
    "station": "rewa",
    "experiment_name": "xgb_aws_prod",
    "model_name": "xgboost-8features-hpt",
    "model_name_gust": "xgboost-8features-hpt-gust",
    "version": str(max_version),
    "version_gust": str(max_version)
}

connection.close()

def call_predict():
    response = requests.post(f'{EC2_ENDPOINT}/predict', json=PARAMS)
    print(response.text)

def call_monitor():
    response = requests.post(f'{EC2_ENDPOINT}/monitor', json=PARAMS)
    print(response.text)

def call_retrain():
    response = requests.post(f'{EC2_ENDPOINT}/retrain', json=PARAMS)
    print(response.text)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python task_runner.py [predict|monitor|retrain]")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == 'predict':
        call_predict()
    elif command == 'monitor':
        call_monitor()
    elif command == 'retrain':
        call_retrain()
    else:
        print("Invalid command. Use 'predict', 'monitor', or 'retrain'.")