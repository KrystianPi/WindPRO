from data.load import select_forecast, select_measurments
from data.ingest import ingest_forecast, ingest_hist_forecast, ingest_measurments
from models.model import Model
import pandas as pd
import mlflow
import sys
import datetime

# Trigger Everyday
def predict(station, RUN_ID):
    ingest_forecast()
    df_forecast = select_forecast(station,purpose='predict')
    model = Model(station='rewa', RUN_ID=RUN_ID, model_name="xgboost-8features-hpt", version=2)
    X = df_forecast[model.feature_names]
    model.predict(X)

# Trigger Every Week
def monitor(station, RUN_ID):
    # Every run check is performed: 
    # If today - last update date in database is less then 7 days replace the week_temp database 
    # If today - last update date => 7 days take the week_temp and append to main 
    # This will prevent duplicates existing in database, every week the forcast and measurments main table will grow by one week of data
    ingest_hist_forecast(past_days=7)
    ingest_measurments(station=station, past_days=7)
    df_forecast = select_forecast(station, purpose='test')
    df_measurments = select_measurments(station, purpose='test')
    
    df_test = pd.merge(df_forecast, df_measurments, how='inner', on='Time')

    df_test.dropna(inplace=True)

    # Double check on duplicates
    df_test.drop_duplicates(subset='Time', inplace=True)

    print(df_test)

    model = Model(station='rewa', RUN_ID=RUN_ID, model_name="xgboost-8features-hpt", version=2)
    model.model_evaluation(df_test)

# Trigger Every Month
def retrain(station, RUN_ID):
    model = Model(station=station,RUN_ID=RUN_ID, model_name="xgboost-8features-hpt", version=2)
    model.get_train_data() 
    model.transform()  
    model.parameter_tuning() 
    model.k_fold_cross_validation()
    model.fit()
    model.save_model()

if __name__ == '__main__': 
    experiment_name = 'xgb_8features'
    
    try:
        id = mlflow.create_experiment(experiment_name)
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
            predict(station='rewa', RUN_ID=run.info.run_id)
        elif sys.argv[1] == 'mon':
            monitor(station='rewa', RUN_ID=run.info.run_id)
        elif sys.argv[1] == 'ret':
            retrain(station='rewa', RUN_ID=run.info.run_id)