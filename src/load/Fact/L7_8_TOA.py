from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/L7_8_TOA_transformed.csv")

sql_query=""" select * from Fact_Count_L7_8_TOA where Ngay_mo_lop >= DATEADD(MONTH, -5, GETDATE())
          """
df_target=transformer.fetch_from_sql_server(sql_query)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_source["Ngay_mo_lop"] = pd.to_datetime(df_source["Ngay_mo_lop"]).dt.date

df_target=transformer.fetch_from_sql_server(sql_query)
df_target["Ngay_mo_lop"] = pd.to_datetime(df_target["Ngay_mo_lop"]).dt.date

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["Ngay_mo_lop", "Ma_lop_hoc","Ma_marketer","Ma_saler","Ma_kenh"], table_name="Fact_Count_L7_8_TOA")
syncer.sync()

