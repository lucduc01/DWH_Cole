from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu mã phân loại trong bảng products Mysql
mysql_query = """select id,code ,status
                from products          
                """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/product.csv",index=False)
