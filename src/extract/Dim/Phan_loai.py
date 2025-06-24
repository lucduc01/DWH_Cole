from Get_data_DB import DataTransformer

# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu mã phân loại trong bảng products Mysql
mysql_query = "select classify from products where status =0"
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/classify.csv",index=False)