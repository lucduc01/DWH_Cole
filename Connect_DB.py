import os
from dotenv import load_dotenv
import mysql.connector
import pyodbc

# Load environment variables from .env file
load_dotenv()

# MySQL Connection
mysql_config = {
    'host': os.getenv('Host_ME'),
    'port': int(os.getenv('Port_ME')),
    'user': os.getenv('User_ME'),
    'password': os.getenv('Pass_ME'),
    'database': os.getenv('Db_ME')
}
mysql_conn = mysql.connector.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# SQL Server Connection
sql_server_config = {
    'server': os.getenv('Host_DWH'),
    'port': int(os.getenv('Port_DWH')),
    'user': os.getenv('User_DWH'),
    'password': os.getenv('Pass_DWH'),
    'database': os.getenv('Db_DWH')
}
sql_server_conn = pyodbc.connect(
   f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server_config['server']};PORT={sql_server_config['port']};DATABASE={sql_server_config['database']};UID={sql_server_config['user']};PWD={sql_server_config['password']}"
)
sql_server_cursor = sql_server_conn.cursor()

# Make connections and cursors available for import
__all__ = ['mysql_conn', 'mysql_cursor', 'sql_server_conn', 'sql_server_cursor']