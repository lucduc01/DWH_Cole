from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Sale_TOT.csv")

sql_query = """ select * from Fact_Ke_hoach_Sale_TOT
            """

df_target=transformer.fetch_from_sql_server(sql_query)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_source["Thoi_gian"] = pd.to_datetime(df_source["Thoi_gian"]).dt.date
df_target["Thoi_gian"] = pd.to_datetime(df_target["Thoi_gian"]).dt.date

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["Thoi_gian", "Ma_khoa_hoc"], table_name="Fact_Ke_hoach_Sale_TOT")
syncer.sync()