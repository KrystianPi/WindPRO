from data.load import select_forecast, select_measurments, select_training_date
from data.ingest import ingest_forecast, ingest_hist_forecast, ingest_measurments, record_training
from models.model import Model
import pandas as pd
import mlflow
import sys
import datetime
import os

# Trigger Everyday
def predict(station, model_name, version, RUN_ID = None):
    '''Inference performed every day. Returns predictions and datetime list'''
    ingest_forecast()
    df_forecast = select_forecast(station,purpose='predict')
    model = Model(station=station,RUN_ID=RUN_ID, model_name=model_name, version=version)
    time = df_forecast['Time'].tolist()
    direction = df_forecast['WindDirForecast']
    X = df_forecast[model.feature_names]
    return model.predict(X), time, direction

# Trigger Every Week
def monitor(station, model_name, version, RUN_ID = None, mode = 'base'):
    '''Function to evaluate the model performance once per week. Return scores for forecast and model result
    If today - last update date in database is less then 7 days replace the week_temp database 
    If today - last update date => 7 days take the week_temp and append to main 
    This will prevent duplicates existing in database, every week the forcast and measurments main table will grow by one week of data '''
    # Calculate the difference in days
    last_training_date = select_training_date(station, model_name)
    difference = (datetime.datetime.now().date() - last_training_date).days
    if difference == 0:
        past_days = 1
    else:
        past_days = difference
    ingest_hist_forecast(past_days=past_days, forecast_days=1)
    ingest_measurments(station=station, past_days=past_days)
    df_forecast = select_forecast(station, past_days=past_days, purpose='test')
    df_measurments = select_measurments(station, past_days=past_days, purpose='test')
    
    df_test = pd.merge(df_forecast, df_measurments, how='inner', on='Time')

    df_test.dropna(subset='WindSpeed', inplace=True)
    df_test.dropna(subset='WindGust', inplace=True)

    # Double check on duplicates
    df_test.drop_duplicates(subset='Time', inplace=True)

    model = Model(station=station,RUN_ID=RUN_ID, model_name=model_name, version=version)
    return model.model_evaluation(df_test, mode)

# Trigger Every Month
def retrain(station, model_name, version, RUN_ID = None, mode = 'base'):
    '''Retrain performed twice per month. Returns train cross validation score.'''
    model = Model(station=station,RUN_ID=RUN_ID, model_name=model_name, version=version)
    df_forecast = select_forecast(station, purpose='retrain')
    df_measurments = select_measurments(station, purpose='retrain')
    #model.get_train_data() 
    model.transform(df_forecast, df_measurments, mode)  
    del df_forecast
    del df_measurments
    model.parameter_tuning() 
    train_cv_accuracy = model.k_fold_cross_validation()
    model.fit()
    model.save_model()
    record_training(station, model_name)
    del model
    return train_cv_accuracy

if __name__ == '__main__': 
    TRACKING_SERVER_HOST = os.environ.get("EC2_TRACKING_SERVER_HOST")
    print(f"Tracking Server URI: '{TRACKING_SERVER_HOST}'")
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:5000") 
    experiment_name = 'xgb_8features_local_testing'
    model_name = 'xgboost-8features-hpt'
    version = 5
    try:
        id = mlflow.create_experiment(experiment_name, artifact_location="s3://mlflow-artifacts-krystianpi")
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    
    today = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    if sys.argv[1] == 'pred':
        run_name = f'pred_run1_{today}'
    elif sys.argv[1] == 'mon':
        run_name = f'test_run1_{today}'
    elif sys.argv[1] == 'ret':
        run_name = f'train_run1_{today}'
    
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run:  
        if sys.argv[1] == 'pred':
            predict(station='rewa', model_name=model_name, version=version, RUN_ID=run.info.run_id)
        elif sys.argv[1] == 'mon':
            monitor(station='rewa', model_name=model_name, version=version, RUN_ID=run.info.run_id)
        elif sys.argv[1] == 'ret':
            retrain(station='rewa', model_name=model_name, version=version, RUN_ID=run.info.run_id)