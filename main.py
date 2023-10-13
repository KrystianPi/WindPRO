from data.load import select_forecast, select_measurments
from models.train import Model
import pandas as pd

def retrain():
    pass

def predict():
    pass

def monitor(station, RUN_ID):
    df_forecast = select_forecast(station, purpose='test')
    df_measurments = select_measurments(station, purpose='test')
    
    df_test = pd.merge(df_forecast, df_measurments, how='inner', on='Time')
    print(df_test.columns)
    
    ######## TEMP FIX ############
    df_test['WindMeasured'] = df_test['WindSpeed']
    df_test.drop_duplicates(subset='Time', inplace=True)
    ####### TEMP FIX #############

    model = Model(station='rewa', RUN_ID=RUN_ID)
    model.model_evaluation(df_test)


if __name__ == '__main__': 
    monitor(station='rewa', RUN_ID='1b96b8edbddb4e95866a5431b25becd0')