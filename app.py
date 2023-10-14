from fastapi import FastAPI
import uvicorn
from main import predict, monitor, retrain

app = FastAPI()

@app.post("/predict")
def api_predict(station: str = 'rewa', model_name: str = 'xgboost-8features-hpt', version: int = 3):
    """ This will be executed once per day """
    predictions = predict(station, model_name, version)
    return {"message": "Prediction completed!", "predictions": predictions}

@app.post("/monitor")
def api_monitor(station: str = 'rewa', model_name: str = 'xgboost-8features-hpt', version: int = 3):
    """ This will be executed once per week """
    monitor(station, model_name, version)
    return {"message": "Monitor completed!"}

@app.post("/retrain")
def api_retrain(station: str = 'rewa', model_name: str = 'xgboost-8features-hpt', version: int = 3):
    """ This will be executed once per month """
    retrain(station, model_name, version)
    return {"message": "Retraining completed!"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
