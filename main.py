from data.load import select_forecast, select_measurments
from data.ingest import ingest_forecast, ingest_hist_forecast, ingest_measurments
from models.train import Model
import pandas as pd
import mlflow
import sys

# Trigger Everyday
def predict(station, RUN_ID):
    ingest_forecast()
    df_forecast = select_forecast(station,purpose='predict')
    model = Model(station='rewa', RUN_ID=RUN_ID)
    X = df_forecast[model.feature_names]
    model.predict(X)

# Trigger Every Week
def monitor(station, RUN_ID):
    ingest_hist_forecast(past_days=7)
    ingest_measurments(past_days=7)
    df_forecast = select_forecast(station, purpose='test')
    df_measurments = select_measurments(station, purpose='test')
    
    df_test = pd.merge(df_forecast, df_measurments, how='inner', on='Time')

    df_test.dropna(inplace=True)

    df_test.drop_duplicates(subset='Time', inplace=True)

    print(df_test)

    model = Model(station='rewa', RUN_ID=RUN_ID)
    model.model_evaluation(df_test)

# Trigger Every Month
def retrain(station):
    # Initate the model with default parameters and perform grid search 
    params_grid = {
        'max_depth': [2, 3, 4,5,6],
        'learning_rate': [0.005,0.01, 0.1, 0.2],
        'n_estimators': [30,50, 100, 200],
    }

    model = Model(station=station)
    model.get_train_data() 
    model.transform()  
    model.parameter_tuning(params_grid) 
    model.k_fold_cross_validation()
    model.fit()
    model.save_model()

if __name__ == '__main__': 
    experiment_name = 'xgb_8features'
    
    try:
        id = mlflow.create_experiment(experiment_name)
    except:
        id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    
    if sys.argv[1] == 'pred':
        run_name = 'pred_run1'
    elif sys.argv[1] == 'mon':
        run_name = 'test_run1'
    elif sys.argv[1] == 'ret':
        run_name = 'train_run1'
    
    with mlflow.start_run(experiment_id=id ,run_name=run_name) as run:  
        if sys.argv[1] == 'pred':
            predict(station='rewa', RUN_ID='1b96b8edbddb4e95866a5431b25becd0')
        elif sys.argv[1] == 'mon':
            monitor(station='rewa', RUN_ID='1b96b8edbddb4e95866a5431b25becd0')
        elif sys.argv[1] == 'ret':
            retrain(station='rewa')