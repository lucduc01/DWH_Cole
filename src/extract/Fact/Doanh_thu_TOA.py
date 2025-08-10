from src.Get_data_DB import DataTransformer
from datetime import datetime
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql
mysql_query = f"""
                select  s.id as Id, 
                        date(o2.open_at) Ngay_mo_lop,
                        t.money Doanh_thu,
                        s.email Ten_hoc_sinh,
                        o2.id AS Ma_lop_hoc,
                        p.code as Ten_san_pham,
                        o.current_sale_id AS Ma_saler,
                        l.utm_medium AS Ma_marketer,
                        o.source AS Ma_kenh
                FROM transactions t
                join transactions_products t2 on t2.p_transaction_id =t.id
                join products p on t2.product_id =p.id
                JOIN orders o ON t.order_id = o.id
                JOIN leads l ON o.lead_id = l.id
                join users u on o.current_sale_id=u.id
                join students s on s.order_id =o.id
                join offline_classes o2 on s.offline_class_id =o2.id
                WHERE   t.t_status = 1
                    And u.role=3
                    and o2.open_at is not null
                    and date(o2.open_at) >= '2024-01-01'
                group by Id, Ngay_mo_lop ,Doanh_thu ,Ten_hoc_sinh ,Ma_lop_hoc,Ten_san_pham,Ma_saler ,Ma_marketer, Ma_kenh
"""
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Doanh_thu_TOA.csv", index=False)
