from datetime import datetime
from typing import Dict, List
import pandas as pd
from Connect_DB import sql_server_conn, sql_server_cursor

class SCDType1SyncFact:
    def __init__(self, df_source, df_target, key_column, table_name):
        self.df_source = df_source.copy()
        self.df_target = df_target.copy()
        self.key_column = key_column
        
        # Xác định các cột cần so sánh (loại bỏ key)
        self.compare_columns = [col for col in self.df_source.columns if col != self.key_column]
        
        # Kết nối SQL Server
        self.sql_server_conn = sql_server_conn
        self.sql_server_cursor = sql_server_cursor
        
        # Tên bảng trong SQL Server cần đồng bộ
        self.table_name = table_name

    def _get_updates(self):
        if not self.compare_columns:
            return pd.DataFrame(columns=[self.key_column])

        df_merge = pd.merge(
            self.df_source,
            self.df_target,
            on=self.key_column,
            how='inner',
            suffixes=('_src', '_tgt')
        )

        # Điều kiện để xác định hàng cần cập nhật
        cond = pd.Series([False] * len(df_merge), index=df_merge.index)
        for col in self.compare_columns:
            cond |= (df_merge[f'{col}_src'] != df_merge[f'{col}_tgt'])
        
        return df_merge[cond][[self.key_column] + [f'{col}_src' for col in self.compare_columns]]

    def _get_inserts(self):
        ids_target = set(self.df_target[self.key_column])
        return self.df_source[~self.df_source[self.key_column].isin(ids_target)]

    def sync(self):
        updates = self._get_updates()
        inserts = self._get_inserts()
        
        # Cập nhật các bản ghi đã thay đổi
        if not updates.empty:
            set_clause = ', '.join([f"{col} = ?" for col in self.compare_columns])
            for i in range(0, len(updates), 100):  # Batch 100 records
                batch = updates.iloc[i:i + 100]
                for _, row in batch.iterrows():
                    sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {self.key_column} = ?"
                    params = [row[f'{col}_src'] for col in self.compare_columns] + [row[self.key_column]]
                    self.sql_server_cursor.execute(sql, params)

        # Thêm mới các bản ghi
        if not inserts.empty:
            for i in range(0, len(inserts), 100):  # Batch 100 records
                batch = inserts.iloc[i:i + 100]
                for _, row in batch.iterrows():
                    cols = ', '.join(inserts.columns)
                    placeholders = ', '.join(['?'] * len(row))
                    sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
                    self.sql_server_cursor.execute(sql, tuple(row))

        # Commit tất cả các thay đổi
        self.sql_server_conn.commit()
        self.sql_server_conn.close()