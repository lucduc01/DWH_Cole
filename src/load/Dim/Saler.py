from src.Get_data_DB import DataTransformer
import pandas as pd
from src.SCD1 import SCDType1Sync

# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu đã xử lý
df_src=pd.read_csv('~/DWH_Cole_Project/data_result/saler_transformed.csv')

# Lấy dữ liệu từ Dim 
sql_query = "select * from Dim_Saler"
df_des = transformer.fetch_from_sql_server(sql_query)

# Đồng bộ dữ liệu 
syncer = SCDType1Sync(df_src, df_des, key_column='Ma_nhan_vien',table_name="Dim_Saler")
syncer.sync()



