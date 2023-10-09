import pandas as pd
from sqlalchemy import create_engine
import json
import pickle

from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
import xgboost as xgb

class Model():
    def __init__(self, station, params):
        self.station = station
        self.model = xgb.XGBRegressor(**params)

    def get_data(self):
        # Read the MySQL configuration from the JSON file
        with open('config.json', 'r') as config_file:
            config = json.load(config_file)

        # Extract MySQL connection details
        mysql_config = config.get('mysql', {})
        username = mysql_config.get('username', 'default_username')
        password = mysql_config.get('password', 'default_password')
        host = mysql_config.get('host', 'localhost')
        database_name = mysql_config.get('database_name', 'your_database')

        # Create the MySQL database connection string
        db_url = f"mysql+mysqlconnector://{username}:{password}@{host}/{database_name}"

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

    def cross_val(self):
        # Features and labels split
        self.X = self.df[['WindForecast', 'WindDirForecast', 'Month', 'GustForecast', 'Hour']]
        self.y = self.df['WindMeasured']

        k_fold = KFold(n_splits=5, shuffle=True, random_state=42)

        # Perform cross-validation and calculate mean and standard deviation of scores
        scores = cross_val_score(self.model, self.X, self.y, cv=k_fold, scoring='neg_mean_squared_error')
        mean_score = -scores.mean()
        std_score = scores.std()
        print(f'Mean Score: {mean_score}')
        print(f'Std Score: {std_score}')
        return mean_score, std_score

    def fit(self):
        self.model.fit(self.X_train, self.y_train)

    def save_model(self, path):
        with open(path, 'wb') as file:
            pickle.dump(self.model, file)

if __name__ == '__main__': 
    
    params = {
    'objective': 'reg:squarederror',
    'n_estimators': 100,
    'learning_rate': 0.1,
    'max_depth': 3,
    }

    model_instance = Model(params)   
    model_instance.get_data() 
    model_instance.transform()  
    mean_score, std_score = model_instance.cross_val() 
    model_instance.fit()
    model_instance.save_model(path='model.pkl')