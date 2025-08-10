from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql
mysql_query = """select s.id as Id, 
                            date(o2.open_at) Ngay_mo_lop,
                            s.money Doanh_so,
                            s.email Ten_hoc_sinh,
                            p.code  Ten_san_pham,
                            o2.id  Ma_lop_hoc,
                            o.current_sale_id  Ma_saler,
                            l.utm_medium  Ma_marketer,
                            o.source  Ma_kenh
                FROM  orders o 
                join orders_products op  on op.order_id =o.id
                join products p on op.product_id=p.id
                JOIN leads l ON o.lead_id = l.id
                join users u on o.current_sale_id=u.id
                join students s on s.order_id =o.id
                join offline_classes o2 on s.offline_class_id =o2.id
                WHERE u.role=3
                and o2.open_at is not null
                and date(o2.open_at) >= '2024-01-01'
                group by Id, Ngay_mo_lop ,Doanh_so ,Ten_hoc_sinh ,Ma_lop_hoc,Ten_san_pham,Ma_saler ,Ma_marketer, Ma_kenh
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOA.csv",index=False)