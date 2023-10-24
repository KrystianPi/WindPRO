from fastapi import FastAPI
import mlflow
from mlflow.tracking import MlflowClient
import uvicorn
from main import predict, monitor, retrain
import datetime
import os

TRACKING_SERVER_HOST = os.environ.get("EC2_TRACKING_SERVER_HOST")
print(f"Tracking Server URI: '{TRACKING_SERVER_HOST}'")
mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000") 


today = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')

app = FastAPI()

@app.post("/predict")
def api_predict(station: str = 'rewa',experiment_name: str = 'xgb_hpt_cv_x1_prod', model_name: str = 'xgboost-8features-hpt', version: int = 2):
    """ This will be executed once per day """
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'pred_run_prod_{today}'
    print(f"default artifacts URI: '{mlflow.get_artifact_uri()}'")
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        predictions = predict(station, model_name, version, run.info.run_id)
    return {"message": "Prediction completed!", "predictions": predictions}

@app.post("/monitor")
def api_monitor(station: str = 'rewa',experiment_name: str = 'xgb_hpt_cv_x1_prod', model_name: str = 'xgboost-8features-hpt', version: int = 2):
    """ This will be executed once per week """
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'test_run_prod_{today}'
    print(f"default artifacts URI: '{mlflow.get_artifact_uri()}'")
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        r2_test, r2_forecast = monitor(station, model_name, version, run.info.run_id)
    return {"message": "Monitor completed!", "r2 score": r2_test, "r2 score forecast": r2_forecast}

@app.post("/retrain")
def api_retrain(station: str = 'rewa',experiment_name: str = 'xgbo_hpt_cv_x1_prod', model_name: str = 'xgboost-8features-hpt', version: int = 2):
    """ This will be executed once per month """
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_name = f'retrain_run_prod_{today}'
    print(f"default artifacts URI: '{mlflow.get_artifact_uri()}'")
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run: 
        train_cv_accuracy = retrain(station, model_name, version, run.info.run_id)
    return {"message": "Retraining completed!", "Train CV Accuracy": train_cv_accuracy}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
