import json
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

def get_config():
    # Read the MySQL configuration from the JSON file
    with open(os.path.join(BASE_DIR, 'config.json'), 'r') as config_file:
        config = json.load(config_file)

    # Extract MySQL connection details
    mysql_config = config.get('mysql', {})
    username = mysql_config.get('username', 'default_username')
    password = mysql_config.get('password', 'default_password')
    host = mysql_config.get('host', 'localhost')
    database_name = mysql_config.get('database_name', 'your_database')

    # Create the MySQL database connection string
    db_url = f"mysql+mysqlconnector://{username}:{password}@{host}/{database_name}"
    
    return db_url