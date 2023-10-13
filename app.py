from fastapi import FastAPI
import uvicorn
from main import predict, monitor, retrain

app = FastAPI()

@app.post("/predict")
def api_predict(station: str = 'rewa', RUN_ID: str = 'ed005057302f4018a8bb1c0d50459e99'):
    predict(station, RUN_ID)
    return {"message": "Prediction completed!"}

@app.post("/monitor")
def api_monitor(station: str = 'rewa', RUN_ID: str = 'ed005057302f4018a8bb1c0d50459e99'):
    monitor(station, RUN_ID)
    return {"message": "Monitor completed!"}

@app.post("/retrain")
def api_retrain(station: str = 'rewa'):
    retrain(station)
    return {"message": "Retraining completed!"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
