import pandas as pd
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta
from typing import Optional

class FactMergerSync:
    def __init__(
        self,
        df_source: pd.DataFrame,
        table_name: str,
        key_column: str,
        engine,
        temp_table_name: str = "temp_fact_sync",
        filter_column: Optional[str] = None
    ):
        """
        df_source: pandas DataFrame cần đồng bộ
        table_name: tên bảng đích trong SQL Server
        key_column: tên cột khoá chính (vd: 'id')
        engine: SQLAlchemy engine kết nối SQL Server
        temp_table_name: tên bảng tạm (tạm thời ghi dữ liệu từ pandas)
        filter_column: cột thời gian để tự động lọc 3 tháng gần nhất (vd: 'Ngày chuyển tiền')
        """
        self.df_source = df_source.copy()
        self.table_name = table_name
        self.key_column = key_column
        self.engine = engine
        self.temp_table_name = temp_table_name
        self.filter_column = filter_column

        # Xác định các cột cần so sánh (trừ key)
        self.compare_columns = [col for col in self.df_source.columns if col != key_column]

    def _filter_last_3_months(self):
        """Tự động lọc dữ liệu 3 tháng gần nhất theo filter_column"""
        if not self.filter_column or self.filter_column not in self.df_source.columns:
            return

        self.df_source[self.filter_column] = pd.to_datetime(self.df_source[self.filter_column])
        max_date = self.df_source[self.filter_column].max()
        min_date = max_date - relativedelta(months=3)

        self.df_source = self.df_source[
            (self.df_source[self.filter_column] >= min_date) &
            (self.df_source[self.filter_column] <= max_date)
        ]

    def _upload_temp_table(self):
        """Ghi df_source lên SQL Server (bảng tạm)"""
        self.df_source.to_sql(
            name=self.temp_table_name,
            con=self.engine,
            if_exists='replace',
            index=False
        )

    def _generate_merge_sql(self):
        """Sinh câu lệnh MERGE SQL"""
        set_clause = ', '.join([f"target.[{col}] = source.[{col}]" for col in self.compare_columns])
        insert_columns = ', '.join([f"[{col}]" for col in self.df_source.columns])
        insert_values = ', '.join([f"source.[{col}]" for col in self.df_source.columns])
        match_condition = f"target.[{self.key_column}] = source.[{self.key_column}]"

        compare_cond = ' OR '.join([
            f"ISNULL(target.[{col}], '') <> ISNULL(source.[{col}], '')" for col in self.compare_columns
        ])

        sql = f"""
        MERGE {self.table_name} AS target
        USING {self.temp_table_name} AS source
        ON {match_condition}
        WHEN MATCHED AND ({compare_cond}) THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
        """
        return sql

    def sync(self):
        """Thực hiện đồng bộ bằng MERGE SQL"""
        self._filter_last_3_months()

        if self.df_source.empty:
            print("Không có dữ liệu để đồng bộ.")
            return

        self._upload_temp_table()

        merge_sql = self._generate_merge_sql()

        with self.engine.begin() as conn:
            conn.execute(merge_sql)

        print("✅ Đồng bộ thành công bằng MERGE SQL.")
