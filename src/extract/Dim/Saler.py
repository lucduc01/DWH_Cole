from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu mã phân loại trong bảng products Mysql
mysql_query = """select id Ma_nhan_vien, 
                        name Ten_nhan_vien, 
                        user_name Ten_he_thong,
                        locked Trang_thai
                 from users
                 where role =3"""
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/saler.csv",index=False)