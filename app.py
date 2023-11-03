from fastapi import FastAPI
import mlflow
from mlflow.tracking import MlflowClient
import uvicorn
import pandas as pd
from main import predict, monitor, retrain
from data.ingest import ingest_predictions_temp
import datetime
import os
from pydantic import BaseModel

class PredictionParams(BaseModel):
    station: str
    experiment_name: str
    model_name: str
    version: int

TRACKING_SERVER_HOST = os.environ.get("EC2_TRACKING_SERVER_HOST")
print(f"Tracking Server URI: '{TRACKING_SERVER_HOST}'")
mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000") 

today = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

app = FastAPI()

@app.post("/predict")
def api_predict(params: PredictionParams):
    """ This will be executed once per day """
    station = params.station
    experiment_name = params.experiment_name
    model_name = params.model_name
    version = params.version

    print(f'Running a prediction for {station}, experiment name: {experiment_name} with model {model_name} v{version}.')
    try:
        print('Trying to create an experiment...')
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        print(f'Experiment {experiment_name} exists')
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'pred_run_prod_{today}'
    print(f'Run name: {run_name}')
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        predictions, time = predict(station, model_name, version, run.info.run_id)
    print(f'Predictions: {predictions}')

    df = pd.DataFrame()
    df['Time'] = time
    df['Wind'] = predictions

    ingest_predictions_temp(station=station, pred=df)

    return {"message": "Prediction completed!", "predictions": predictions, "time": time}

@app.post("/monitor")
def api_monitor(params: PredictionParams):
    """ This will be executed once per week """
    station = params.station
    experiment_name = params.experiment_name
    model_name = params.model_name
    version = params.version
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'test_run_prod_{today}'
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        r2_test, r2_forecast = monitor(station, model_name, version, run.info.run_id)
    return {"message": "Monitor completed!", "r2 score": r2_test, "r2 score forecast": r2_forecast}

@app.post("/retrain")
def api_retrain(params: PredictionParams):
    """ This will be executed once per month """
    station = params.station
    experiment_name = params.experiment_name
    model_name = params.model_name
    version = params.version
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'retrain_run_prod_{today}'
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        train_cv_accuracy = retrain(station, model_name, version, run.info.run_id)
    return {"message": "Retraining completed!", "Train CV Accuracy": train_cv_accuracy}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
