from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql
mysql_query = """SELECT  soh.sale_order_level_id Level_Id,
                        date(oc.open_at) Ngay_mo_lop,
                        oc.id Ma_lop_hoc,
                        o.source  Ma_kenh,
                        l.utm_medium Ma_marketer,
                        o.current_sale_id Ma_saler
                from sale_order_histories soh 
                join orders o on soh.order_id =o.id
                join users u on o.current_sale_id =u.id
                join leads l on l.id=o.lead_id
                join students s on s.order_id =o.id
                join offline_classes oc on s.offline_class_id = oc.id 
                where soh.sale_order_level_id in (16,19)
                and oc.open_at is not null
                and u.role=3
                and date(oc.open_at) >= '2024-01-01'
                group by Ngay_mo_lop ,Ma_lop_hoc , Ma_kenh , Ma_marketer ,Ma_saler ,Level_Id
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/L7_8_TOA.csv",index=False)