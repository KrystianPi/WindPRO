import pandas as pd
from sqlalchemy import create_engine
import warnings

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
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
        self.feature_names = ['Month', 'Hour', 'WindForecast', 'GustForecast',	
                              'WindDirForecast', 'Temperature', 'Precipitation', 'Cloudcover']   
        try:
            self.load_model()
        except Exception as e:
            print(f'Model not found initiating default model and training: {str(e)}')
            

    def get_train_data(self):
        db_url = get_config()

        # Create an SQLAlchemy engine
        engine = create_engine(db_url)

        # Use the engine to connect to the database
        connection = engine.connect()

        # Specify the SQL query to retrieve data from a table
        query_forecast = f"SELECT * FROM forecast"
        query_measurments = f"SELECT * FROM measurments_{self.station}"

        # Use Pandas to read data from the database into a DataFrame
        self.df_forecast = pd.read_sql(query_forecast, connection)
        self.df_measurments = pd.read_sql(query_measurments, connection)

        connection.close()

    def transform(self, df_measurments, df_forecast, mode='base'):
        # Set the 'Time' column as the index
        df_measurments.set_index('Time', inplace=True)

        # Resample the data with a two-hour interval and apply mean aggregation
        df_measurments = df_measurments.resample('2H').mean()

        df_measurments.reset_index(inplace=True)

        df = pd.merge(left=df_forecast, right=df_measurments, on='Time', how='inner')

        df.dropna(inplace=True)

        # Close the database connections
        self.df = df
        
        # Features and labels split
        self.X = self.df[self.feature_names]
        if mode == 'base':
            self.y = self.df['WindSpeed']
        elif mode =='gust':
            self.y = self.df['WindGust']
   
    def parameter_tuning(self):
        self.k_fold = KFold(n_splits=5, shuffle=True, random_state=42)

        self.params_grid = {
         'max_depth': [2, 3],
         'learning_rate': [0.005,0.01, 0.1],
         'n_estimators': [30, 50],
            }
        
        # Grid search
        xgb_regressor = xgb.XGBRegressor()
        grid = GridSearchCV(xgb_regressor, self.params_grid, cv=self.k_fold)
        grid.fit(self.X, self.y)

        self.best_max_depth = grid.best_params_['max_depth']
        self.best_learning_rate = grid.best_params_['learning_rate']
        self.best_n_estimators = grid.best_params_['n_estimators']
        # Add other best hyperparameters as needed

        mlflow.log_param(f'best_max_depth', self.best_max_depth)
        mlflow.log_param(f'best_learning_rate', self.best_learning_rate)
        mlflow.log_param(f'best_n_estimators', self.best_n_estimators)
        mlflow.log_param(f'param_grid', self.params_grid)

        self.model = xgb.XGBRegressor(
                                    max_depth=self.best_max_depth,
                                    learning_rate=self.best_learning_rate,
                                    n_estimators=self.best_n_estimators,
                                    )
        
    def k_fold_cross_validation(self):
        # Run k-fold cross validation
        kfold_scores = cross_val_score(self.model, self.X, self.y, cv=self.k_fold)

        # Track metrics
        mlflow.log_metric(f"average_accuracy", kfold_scores.mean())
        mlflow.log_metric(f"std_accuracy", kfold_scores.std())
        return kfold_scores.mean()

    def fit(self):
        self.model.fit(self.X, self.y)

    def save_model(self):
        #mlflow.register_model(f"s3://mlflow-artifacts-krystianpi/{mlflow.active_run().info.run_id}/sklearn-model", self.model_name)
        #mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/sklearn-model", self.model_name)
        mlflow.register_model(
        f"runs:/{self.id}/sklearn-model", self.model_name
        )
        mlflow.sklearn.log_model(self.model, "sklearn-model")

    def predict(self, X):
        pred = self.model.predict(X)
        # Convert predictions to a suitable format (e.g., a Python list or dictionary)
        X['Predicition'] = pred
        print(X)
        return(pred.tolist())

    def model_evaluation(self, test_data, mode='base'):
        X_test = test_data[self.feature_names]
        if mode == 'base':
            y_forecast = test_data['WindForecast']
            y_test = test_data['WindSpeed']
        elif mode == 'gust':
            y_forecast = test_data['GustForecast']
            y_test = test_data['WindGust']
        y_pred = self.model.predict(X_test)
        test_data['Prediction'] = y_pred
        print(test_data[['Time','WindForecast', 'GustForecast','WindSpeed','WindGust','Prediction']])
        mlflow.log_metric(f"test_accuracy", r2_score(y_test, y_pred))
        mlflow.log_metric(f"forecast_accuracy", r2_score(y_test, y_forecast))
        mlflow.log_param("model", mode)
        mlflow.log_param("station", self.station)
        mlflow.log_param("Date Range min", test_data['Time'].min())
        mlflow.log_param("Date Range max", test_data['Time'].max())
        return r2_score(y_test, y_pred), r2_score(y_test, y_forecast)

    def load_model(self):
        # Load a trained model from a pickle file
        self.model = mlflow.pyfunc.load_model(model_uri=f"models:/{self.model_name}/{self.version}")

if __name__ == '__main__': 
    pass
