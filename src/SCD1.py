from datetime import datetime
from typing import Dict, List
import pandas as pd
from Connect_DB import sql_server_conn, sql_server_cursor

class SCDType1Sync:
    def __init__(self, df_source, df_target, key_column,table_name):
        self.df_source = df_source.copy()
        self.df_target = df_target.copy()
        self.key_column = key_column

        # Tự động xác định các cột cần so sánh (loại bỏ key)
        self.compare_columns = [col for col in self.df_source.columns if col != self.key_column]

        # Kết nối SQL Server
        self.sql_server_conn = sql_server_conn
        self.sql_server_cursor = sql_server_cursor

        # Tên bảng trong SQL Server cần đồng bộ
        self.table_name = table_name

    def _get_updates(self):
        df_merge = pd.merge(
            self.df_source,
            self.df_target,
            on=self.key_column,
            how='inner',
            suffixes=('_src', '_tgt')
        )
        cond = False
        for col in self.compare_columns:
            cond |= (df_merge[f'{col}_src'] != df_merge[f'{col}_tgt'])
        return df_merge[cond][[self.key_column] + [f'{col}_src' for col in self.compare_columns]]

    def _get_inserts(self):
        ids_target = set(self.df_target[self.key_column])
        return self.df_source[~self.df_source[self.key_column].isin(ids_target)]

    def _get_deletes(self):
        ids_source = set(self.df_source[self.key_column])
        return self.df_target[~self.df_target[self.key_column].isin(ids_source)]

    def sync(self):
        if self.df_target.empty:
            # Nếu bảng đích trống → INSERT toàn bộ
            for _, row in self.df_source.iterrows():
                cols = ', '.join(self.df_source.columns)
                placeholders = ', '.join(['?'] * len(row))
                sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
                self.sql_server_cursor.execute(sql, tuple(row))
            self.sql_server_conn.commit()
            return

        # Cập nhật
        updates = self._get_updates()
        for _, row in updates.iterrows():
            set_clause = ', '.join([f"{col} = ?" for col in self.compare_columns])
            sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {self.key_column} = ?"
            params = [row[f'{col}_src'] for col in self.compare_columns] + [row[self.key_column]]
            self.sql_server_cursor.execute(sql, params)

        # Thêm mới
        inserts = self._get_inserts()
        for _, row in inserts.iterrows():
            cols = ', '.join(inserts.columns)
            placeholders = ', '.join(['?'] * len(row))
            sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
            self.sql_server_cursor.execute(sql, tuple(row))

        # Xoá
        deletes = self._get_deletes()
        for _, row in deletes.iterrows():
            sql = f"DELETE FROM {self.table_name} WHERE {self.key_column} = ?"
            self.sql_server_cursor.execute(sql, (row[self.key_column],))

        self.sql_server_conn.commit()
        self.sql_server_conn.close()