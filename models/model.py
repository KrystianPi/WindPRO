import pandas as pd
from sqlalchemy import create_engine
import warnings

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.metrics import r2_score
import xgboost as xgb

from .config import get_config
from pathlib import Path

import mlflow
import mlflow.sklearn
import mlflow.pyfunc

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

# Ignore all warnings
warnings.filterwarnings("ignore")

# This class should be only for predicitng, model evaluation and retraining
class Model():
    def __init__(self, station, RUN_ID, model_name, version):
        self.station = station 
        self.id = RUN_ID 
        self.model_name = model_name
        self.version = version
        self.feature_names = ['WindForecast', 'WindDirForecast', 'Month', 'GustForecast', 
                        'Hour', 'Temperature', 'Cloudcover','Precipitation']   
        self.load_model()

    def get_train_data(self):
        db_url = get_config()

        # Create an SQLAlchemy engine
        engine = create_engine(db_url)

        # Use the engine to connect to the database
        connection = engine.connect()

        # Specify the SQL query to retrieve data from a table
        query_forecast = f"SELECT * FROM forecast_{self.station}"
        query_measurments = f"SELECT * FROM measurments_{self.station}"

        # Use Pandas to read data from the database into a DataFrame
        self.df_forecast = pd.read_sql(query_forecast, connection)
        self.df_measurments = pd.read_sql(query_measurments, connection)

        connection.close()

    def transform(self):
        # Set the 'Time' column as the index
        self.df_measurments.set_index('Time', inplace=True)

        # Resample the data with a two-hour interval and apply mean aggregation
        self.df_measurments = self.df_measurments.resample('2H').mean()

        self.df_measurments.reset_index(inplace=True)

        df = pd.merge(left=self.df_forecast, right=self.df_measurments, on='Time', how='inner')

        df.dropna(inplace=True)

        # Close the database connections
        self.df = df
        
        # Features and labels split
        self.X = self.df[self.feature_names]
        self.y = self.df['WindSpeed']
   
    def k_fold_cross_validation(self):
        # Run k-fold cross validation
        self.k_fold = KFold(n_splits=5, shuffle=True, random_state=42)
        kfold_scores = cross_val_score(self.model, self.X, self.y, cv=self.k_fold)

        # Track metrics
        mlflow.log_metric(f"average_accuracy", kfold_scores.mean())
        mlflow.log_metric(f"std_accuracy", kfold_scores.std())

    def fit(self):
        self.model.fit(self.X, self.y)

    def save_model(self):
        mlflow.register_model(
        f"runs:/{self.id}/sklearn-model", "xgboost-8features-hpt"
        )
        mlflow.sklearn.log_model(self.model, "sklearn-model")

    def predict(self, X):
        pred = self.model.predict(X)
        # Convert predictions to a suitable format (e.g., a Python list or dictionary)
        print(pred)
        return(pred)

    def model_evaluation(self, test_data):
        X_test = test_data[self.feature_names]
        y_test = test_data['WindSpeed']
        y_pred = self.model.predict(X_test)
        mlflow.log_metric(f"test_accuracy", r2_score(y_test, y_pred))

    def load_model(self):
        # Load a trained model from a pickle file
        self.model = mlflow.pyfunc.load_model(model_uri=f"models:/{self.model_name}/{self.version}")

if __name__ == '__main__': 
    pass
