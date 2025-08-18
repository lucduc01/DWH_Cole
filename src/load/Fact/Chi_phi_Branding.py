from src.Get_data_DB import DataTransformer
import pandas as pd
from src.FactMergerSync import SCDType1SyncFact

# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu đã xử lý
df_src=pd.read_csv('~/DWH_Cole_Project/data_result/Chi_phi_Branding_transformed.csv')

# Lấy dữ liệu từ Dim 
sql_query = "select * from Fact_Chi_phi_Branding"
df_des = transformer.fetch_from_sql_server(sql_query)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_src["Thoi_gian"] = pd.to_datetime(df_src["Thoi_gian"]).dt.date

df_des["Thoi_gian"] = pd.to_datetime(df_src["Thoi_gian"]).dt.date

# Đồng bộ dữ liệu 
syncer = SCDType1SyncFact(df_src, df_des, key_column='Thoi_gian',table_name="Fact_Chi_phi_Branding")
syncer.sync()

