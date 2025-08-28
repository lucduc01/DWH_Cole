from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1_CompositeKey import SCDType1SyncMultipleKey

transformer=DataTransformer()
#Lấy dữ liệu
df_source=pd.read_csv("~/DWH_Cole_Project/data_result/Chien_dich_meta.csv")

sql_query=""" select * from Chien_dich_Meta where Ngay_bat_dau >= DATEADD(MONTH, -24, GETDATE()) 
          """
df_target=transformer.fetch_from_sql_server(sql_query)

syncer = SCDType1SyncMultipleKey(df_source, df_target, key_columns=["STT", "Account"], table_name="Chien_dich_Meta")
syncer.sync()
