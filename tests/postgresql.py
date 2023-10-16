import psycopg2
import json
import os

BASE_DIR = os.getcwd()

# Read the MySQL configuration from the JSON file
with open(os.path.join(BASE_DIR, 'config.json'), 'r') as config_file:
    config = json.load(config_file)

# Extract MySQL connection details
postgresql_config = config.get('postgresql', {})
username = postgresql_config.get('username', 'default_username')
password = postgresql_config.get('password', 'default_password')
host = postgresql_config.get('host', 'localhost')
database_name = postgresql_config.get('database_name', 'your_database')
port = postgresql_config.get('port', 'your_port')

# Connect to the database
conn = None
try:
    conn = psycopg2.connect(
        host=host,
        database=database_name,
        user=username,
        password=password,
        port=port
    )

    # Create a cursor object using the connection
    cur = conn.cursor()

    # Execute a simple query
    cur.execute('SELECT version();')
    version = cur.fetchone()
    print(f"Connected to - {version[0]}")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the connection and cursor
    if cur:
        cur.close()
    if conn:
        conn.close()