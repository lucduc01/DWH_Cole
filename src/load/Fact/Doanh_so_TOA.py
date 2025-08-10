from src.Get_data_DB import DataTransformer
from src.FactMergerSync import FactMergerSync
import pandas as pd
from sqlalchemy import create_engine


transformer=DataTransformer()

# Lấy dữ liệu đã xử lý
df_src=pd.read_csv('~/DWH_Cole_Project/data_result/Doanh_so_TOA_transformed.csv')

# Đồng bộ dữ liệu
syncer = FactMergerSync(
    df_source=df_src,
    table_name="Fact_Doanh_so_TOA",
    key_column="Id",
    env_file="~/DWH_Cole_Project/.env"
)

syncer.sync()