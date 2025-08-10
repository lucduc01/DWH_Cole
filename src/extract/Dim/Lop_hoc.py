from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lớp học trong bảng offline_classe Mysql
mysql_query = """select oc.id Ma_lop_hoc,
                        oc.code Ten_lop_hoc,
                        oc.open_at Ngay_khai_giang,
                        oc.close_at Ngay_ket_thuc,
                        oc.lessons_num So_buoi_hoc,
                        oc.status Trang_thai,
                        p.id Ma_khoa_hoc
                from offline_classes oc 
                join items i on oc.item_id =i.id
                join products p on p.unique_code=i.unique_code  
                """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/class.csv",index=False)