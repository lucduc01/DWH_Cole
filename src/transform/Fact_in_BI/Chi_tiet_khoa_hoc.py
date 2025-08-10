import pandas as pd
from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Kế thừa từ bảng Tong_ket_so_Marketing
df=pd.read_csv("~/DWH_Cole_Project/data_result/Tong_ket_Marketing.csv")


