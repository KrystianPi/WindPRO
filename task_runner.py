import requests
import sys
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Get the EC2 tracking server host from the environment variable
EC2_TRACKING_SERVER_HOST = os.getenv('EC2_TRACKING_SERVER_HOST')
EC2_ENDPOINT = f"http://{EC2_TRACKING_SERVER_HOST}:8000"


PARAMS = {
    "station": "rewa",
    "experiment_name": "xgb_aws_prod",
    "model_name": "xgboost-8features-hpt",
    "version": 1  
}

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