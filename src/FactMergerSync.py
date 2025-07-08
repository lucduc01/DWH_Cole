import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
from dotenv import load_dotenv
from urllib.parse import quote_plus
import os

class FactMergerSync:
    def __init__(
        self,
        df_source: pd.DataFrame,
        table_name: str,
        key_column: str,
        temp_table_name: str = "temp_fact_sync",
        env_file: str = ".env"
    ):
        """
        df_source: DataFrame cần đồng bộ (đã được lọc 3 tháng gần nhất)
        table_name: tên bảng đích trong SQL Server
        key_column: tên cột khóa chính
        env_file: đường dẫn đến file .env chứa thông tin kết nối
        """
        self.df_source = df_source.copy()
        self.table_name = table_name
        self.key_column = key_column
        self.temp_table_name = temp_table_name
        self.compare_columns = [col for col in self.df_source.columns if col != key_column]

        # Load biến môi trường từ file .env
        load_dotenv(dotenv_path=env_file)
        host = os.getenv("Host_DWH")
        port = os.getenv("Port_DWH", "1433")
        user = os.getenv("User_DWH")
        password = os.getenv("Pass_DWH")
        database = os.getenv("Db_DWH")
        driver = "ODBC Driver 17 for SQL Server"

        # Tạo connection string
        params = quote_plus(
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=60;"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        self.engine = create_engine(connection_string, fast_executemany=True)

    def _upload_temp_table(self):
        self.df_source.to_sql(
            name=self.temp_table_name,
            con=self.engine,
            if_exists='replace',
            index=False
        )

    def _generate_merge_sql(self):
        set_clause = ', '.join([f"target.[{col}] = source.[{col}]" for col in self.compare_columns])
        insert_columns = ', '.join([f"[{col}]" for col in self.df_source.columns])
        insert_values = ', '.join([f"source.[{col}]" for col in self.df_source.columns])
        match_condition = f"target.[{self.key_column}] = source.[{self.key_column}]"
        compare_cond = ' OR '.join([
            f"ISNULL(target.[{col}], '') <> ISNULL(source.[{col}], '')" for col in self.compare_columns
        ])
        return f"""
        MERGE {self.table_name} AS target
        USING {self.temp_table_name} AS source
        ON {match_condition}
        WHEN MATCHED AND ({compare_cond}) THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """

    def sync(self):
        if self.df_source.empty:
            print("⚠️ Không có dữ liệu để đồng bộ.")
            return
        self._upload_temp_table()
        merge_sql = self._generate_merge_sql()
        with self.engine.begin() as conn:
            conn.execute(text(merge_sql))
        print("✅ Đồng bộ thành công bằng MERGE SQL.")
