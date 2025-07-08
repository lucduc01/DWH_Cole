import pandas as pd
from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor

class DataTransformer:
    def __init__(self):
        self.mysql_conn = mysql_conn
        self.mysql_cursor = mysql_cursor
        self.sql_server_conn = sql_server_conn
        self.sql_server_cursor = sql_server_cursor

    def fetch_from_mysql(self, query):
        """Thực thi query từ MySQL và trả về DataFrame."""
        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching from MySQL: {e}")
            return None

    def fetch_from_sql_server(self, query):
        """Thực thi query từ SQL Server và trả về DataFrame."""
        try:
            self.sql_server_cursor.execute(query)
            columns = [desc[0] for desc in self.sql_server_cursor.description]
            data = self.sql_server_cursor.fetchall()
            data = [tuple(row) for row in data]

            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            print(f"Error fetching from SQL Server: {e}")
            return None
