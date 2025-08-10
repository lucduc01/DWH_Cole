from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/Count_Level_TOT_transformed.csv")

sql_query=""" select * from Fact_Count_Level_TOT
          """
df_target=transformer.fetch_from_sql_server(sql_query)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_source["Ngay"] = pd.to_datetime(df_source["Ngay"]).dt.date

df_target=transformer.fetch_from_sql_server(sql_query)
df_target["Ngay"] = pd.to_datetime(df_target["Ngay"]).dt.date

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["Ngay", "Ma_khoa_hoc","Ma_marketer","Ma_saler","Ma_kenh"], table_name="Fact_Count_Level_TOT")
syncer.sync()
