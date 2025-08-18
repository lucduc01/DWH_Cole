from src.Get_data_DB import DataTransformer
from src.FactMergerSync import SCDType1SyncFact
import pandas as pd
from sqlalchemy import create_engine


transformer=DataTransformer()

# Lấy dữ liệu đã xử lý
df_src=pd.read_csv('~/DWH_Cole_Project/data_result/Doanh_thu_TOA_transformed.csv')


# Lấy dữ liệu từ Fact
sql_query = "select * from Fact_Doanh_thu_TOA where Ngay_mo_lop >= DATEADD(MONTH, -6, GETDATE()) "
df_des = transformer.fetch_from_sql_server(sql_query)

# Đồng bộ dữ liệu 
syncer = SCDType1SyncFact(df_src, df_des, key_column='Id',table_name="Fact_Doanh_thu_TOA")
syncer.sync()