import pandas as pd
from sqlalchemy import create_engine
import warnings
import ast

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.model_selection import GridSearchCV
import xgboost as xgb

from .config import get_config
from pathlib import Path

import mlflow
import mlflow.sklearn

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

# Ignore all warnings
warnings.filterwarnings("ignore")

class Model():
    def __init__(self, station, params=None, RUN_ID=None):
        self.station = station
        self.model = None       
        # If run_id is provided, load the model from a mlflow pickle file
        if RUN_ID != None:
            self.load_model(RUN_ID)
        # If params are provided create a new XGBoost regressor with the provided parameters
        elif params:
            self.model = xgb.XGBRegressor(**params)
            self.feature_names = ['WindForecast', 'WindDirForecast', 'Month', 'GustForecast', 
                                  'Hour', 'Temperature', 'Cloudcover','Precipitation'] 
            mlflow.log_params({"feature_names": self.feature_names})
            mlflow.log_param(f'best_max_depth', self.best_max_depth)
            mlflow.log_param(f'best_learning_rate', self.best_learning_rate)
            mlflow.log_param(f'best_n_estimators', self.best_n_estimators)
        # Else create a default model
        else:
            self.model = xgb.XGBRegressor()
            self.feature_names = ['WindForecast', 'WindDirForecast', 'Month', 'GustForecast', 
                                    'Hour', 'Temperature', 'Cloudcover','Precipitation'] 
            mlflow.log_params({"feature_names": self.feature_names})

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

    def parameter_tuning(self, parameters):
        self.k_fold = KFold(n_splits=5, shuffle=True, random_state=42)

        # Grid search
        xgb_regressor = xgb.XGBRegressor()
        grid = GridSearchCV(xgb_regressor, parameters, cv=self.k_fold)
        grid.fit(self.X, self.y)

        self.best_max_depth = grid.best_params_['max_depth']
        self.best_learning_rate = grid.best_params_['learning_rate']
        self.best_n_estimators = grid.best_params_['n_estimators']
        # Add other best hyperparameters as needed

        mlflow.log_param(f'best_max_depth', self.best_max_depth)
        mlflow.log_param(f'best_learning_rate', self.best_learning_rate)
        mlflow.log_param(f'best_n_estimators', self.best_n_estimators)

        self.model = xgb.XGBRegressor(
                                    max_depth=self.best_max_depth,
                                    learning_rate=self.best_learning_rate,
                                    n_estimators=self.best_n_estimators,
                                    )
    
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
        mlflow.sklearn.log_model(self.model, "model")

    def predict(self, X):
        pred = self.model.predict(X)
        print(pred)
        return(pred)

    def model_evaluation(self, test_data):
        X_test = test_data[self.feature_names]
        y_test = test_data['WindSpeed']
        mlflow.log_metric(f"test_accuracy", self.model.score(X_test, y_test))

    def load_model(self, RUN_ID):
        # Load a trained model from a pickle file
        self.model = mlflow.sklearn.load_model(f"runs:/{RUN_ID}/model")
        self.feature_names = ast.literal_eval(mlflow.get_run(RUN_ID).data.params['feature_names'])

if __name__ == '__main__': 
    pass
