import pandas as pd
from typing import List
from Connect_DB import sql_server_conn, sql_server_cursor

class SCDType1SyncMultipleKey:
    def __init__(self, df_source: pd.DataFrame, df_target: pd.DataFrame, key_columns: List[str], table_name: str):
        self.df_source = df_source.copy()
        self.df_target = df_target.copy()
        self.key_columns = key_columns
        self.table_name = table_name

        # Xác định các cột cần so sánh
        self.compare_columns = [col for col in self.df_source.columns if col not in self.key_columns]

    def get_updates(self):
        if not self.compare_columns:
            return pd.DataFrame(columns=self.key_columns)

        df_merge = pd.merge(
            self.df_source,
            self.df_target,
            on=self.key_columns,
            how='inner',
            suffixes=('_src', '_tgt')
        )

        cond = pd.Series(False, index=df_merge.index)
        for col in self.compare_columns:
            cond |= (df_merge[f"{col}_src"] != df_merge[f"{col}_tgt"])

        update_cols = self.key_columns + [f"{col}_src" for col in self.compare_columns]
        return df_merge[cond][update_cols]

    def get_inserts(self):
        if self.df_target.empty:
            return self.df_source
        return self.df_source.merge(self.df_target[self.key_columns], on=self.key_columns, how='left', indicator=True) \
                             .query('_merge == "left_only"') \
                             .drop(columns=['_merge'])

    def apply_insert(self, inserts: pd.DataFrame):
        for _, row in inserts.iterrows():

            cols = ', '.join(inserts.columns)
            placeholders = ', '.join(['?'] * len(row))
            sql = f"INSERT INTO {self.table_name} ({cols}) VALUES ({placeholders})"
            sql_server_cursor.execute(sql, tuple(row))

    """def apply_update(self, updates: pd.DataFrame):
        for _, row in updates.iterrows():
            set_clause = ', '.join([f"{col} = ?" for col in self.compare_columns])
            where_clause = ' AND '.join([f"{col} = ?" for col in self.key_columns])
            sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
            params = [row[f"{col}_src"] for col in self.compare_columns] + [row[col] for col in self.key_columns]
            sql_server_cursor.execute(sql, params)"""
    
    def apply_update(self, updates: pd.DataFrame):
        """Cập nhật tất cả các dòng thay đổi"""
        set_clause = ", ".join([f"{col} = ?" for col in self.compare_columns])
        where_clause = " AND ".join([f"{col} = ?" for col in self.key_columns])
        
        sql = f"UPDATE {self.table_name} SET {set_clause} WHERE {where_clause}"
        
        # Duyệt qua TỪNG DÒNG trong DataFrame updates
        for _, row in updates.iterrows():
            params = []
            
            # Xử lý các cột cần cập nhật
            for col in self.compare_columns:
                val = row[f"{col}_src"]
                # Chuyển đổi numpy types sang Python native types
                if hasattr(val, 'item'):
                    val = val.item()
                elif pd.api.types.is_integer_dtype(type(val)):
                    val = int(val)
                elif pd.api.types.is_float_dtype(type(val)):
                    val = float(val)
                params.append(val)
            
            # Xử lý các key columns cho điều kiện WHERE
            for col in self.key_columns:
                val = row[col]
                if hasattr(val, 'item'):
                    val = val.item()
                elif pd.api.types.is_integer_dtype(type(val)):
                    val = int(val)
                elif pd.api.types.is_float_dtype(type(val)):
                    val = float(val)
                params.append(val)
            
            # Thực thi UPDATE cho mỗi dòng
            sql_server_cursor.execute(sql, params)

    def sync(self):
        # Nếu bảng đích trống, chèn toàn bộ
        if self.df_target.empty:
          
            self.apply_insert(self.df_source)
        else:
            updates = self.get_updates()
         
            inserts = self.get_inserts()
           
            if not updates.empty:
                self.apply_update(updates)
            if not inserts.empty:
                self.apply_insert(inserts)

        sql_server_conn.commit()
        sql_server_conn.close()
