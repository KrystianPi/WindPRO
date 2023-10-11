import pandas as pd
from sqlalchemy import create_engine
import warnings

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.model_selection import GridSearchCV
import xgboost as xgb

from config import get_config
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
            self.feature_names = ['WindForecast', 'WindDirForecast', 'Month', 'GustForecast'] 
            mlflow.log_params({"feature_names": self.feature_names})
            mlflow.log_param(f'best_max_depth', self.best_max_depth)
            mlflow.log_param(f'best_learning_rate', self.best_learning_rate)
            mlflow.log_param(f'best_n_estimators', self.best_n_estimators)
        # Else create a default model
        else:
            self.model = xgb.XGBRegressor()
            self.feature_names = ['WindForecast', 'WindDirForecast', 'Month', 'GustForecast'] 
            mlflow.log_params({"feature_names": self.feature_names})

    def get_train_data(self):
        db_url = get_config()

        # Create an SQLAlchemy engine
        engine = create_engine(db_url)

        # Use the engine to connect to the database
        connection = engine.connect()

        # Specify the SQL query to retrieve data from a table
        query = f"SELECT * FROM joined_{self.station}"

        # Use Pandas to read data from the database into a DataFrame
        df = pd.read_sql(query, connection)

        # Close the database connection
        connection.close()

        self.df = df

    def transform(self):
        # Average
        df_avg = self.df.groupby('RightTableTime')[['WindMeasured', 'WindForecast', 'WindDirForecast', 
                                                'Month', 'CloudForecast','GustForecast','TempForecast',
                                                'PrecipitationForecast', 'WindDirMeasured']] \
                                                .mean().reset_index().dropna()

        # Define wind direction bins
        bins = [0, 45, 90, 135, 180, 225, 270, 315, 360]
        labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

        # Bin wind directions and calculate mean wind speed for each bin
        df_avg['WindDirMeasuredBin'] = pd.cut(df_avg['WindDirMeasured'], bins=bins, labels=labels)
        df_avg['WindDirBinForecast'] = pd.cut(df_avg['WindDirForecast'], bins=bins, labels=labels)

        self.df = df_avg
        
        # Features and labels split
        self.X = self.df[self.feature_names]
        self.y = self.df['WindMeasured']

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
        return(self.model.predict(X))

    def model_evaluation(self, test_data):
        X_test = test_data[self.feature_names]
        y_test = test_data['WindMeasured']
        mlflow.log_metric(f"test_accuracy", self.model.score(X_test, y_test))

    def load_model(self, RUN_ID):
        # Load a trained model from a pickle file
        self.model = mlflow.sklearn.load_model(f"runs:/{RUN_ID}/model")
        self.feature_names = mlflow.get_run(RUN_ID).data.params['feature_names']

if __name__ == '__main__': 
    
    # Initiate the model with set params
    params = {
    'objective': 'reg:squarederror',
    'n_estimators': 100,
    'learning_rate': 0.1,
    'max_depth': 3,
    }

    # Initate the model with default parameters and perform grid search 
    params_grid = {
        'max_depth': [2, 3, 4,5,6],
        'learning_rate': [0.005,0.01, 0.1, 0.2],
        'n_estimators': [30,50, 100, 200],
    }

    with mlflow.start_run() as run:   
        model_instance = Model('rewa') 
        model_instance.get_train_data() 
        model_instance.transform()  
        model_instance.parameter_tuning(params_grid) 
        model_instance.k_fold_cross_validation()
        model_instance.fit()
        model_instance.save_model()

    # model_instance = Model('rewa', RUN_ID='a33c8a70fe804490b5eb5b1bb53377b7') 
    # print(model_instance.model)  
