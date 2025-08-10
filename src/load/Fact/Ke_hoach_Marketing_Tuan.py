from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Marketing_Tuan_transformed.csv")

sql_query = """ select * from Fact_Ke_hoach_Marketing_Tuan
            """

df_target=transformer.fetch_from_sql_server(sql_query)

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["Nam", "Ma_khoa_hoc","Thang","Tuan"], table_name="Fact_Ke_hoach_Marketing_Tuan")
syncer.sync()