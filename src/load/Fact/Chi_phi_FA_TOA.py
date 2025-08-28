from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/Chi_phi_FA_TOA_transformed.csv")

sql_query=""" select * from Fact_Chi_phi_FA_TOA where Thoi_gian >= DATEADD(MONTH, -5, GETDATE())
          """

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_source["Thoi_gian"] = pd.to_datetime(df_source["Thoi_gian"]).dt.date

df_target=transformer.fetch_from_sql_server(sql_query)
df_target["Thoi_gian"] = pd.to_datetime(df_target["Thoi_gian"]).dt.date

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["Thoi_gian", "Ma_lop_hoc","Ma_marketer"], table_name="Fact_Chi_phi_FA_TOA")
syncer.sync()