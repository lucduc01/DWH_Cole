from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql
mysql_query = """select tp.id Id,
                        t.money Doanh_so, 
                        DATE_ADD(t.payment_at, INTERVAL 7 HOUR) as Ngay_chuyen_tien,
                        p.id as Ma_khoa_hoc, 
                        oco.o_customer_id,
                        o.current_sale_order_level_id,
                        o.current_sale_id as Ma_saler,
                        o.source as Ma_kenh,
                        l.utm_medium as Ma_marketer
                    FROM transactions t 
                    JOIN transactions_products tp ON tp.p_transaction_id = t.id
                    JOIN products p ON tp.product_id = p.id
                    JOIN orders o ON t.order_id = o.id
                    join o_customers_orders oco on oco.order_id =o.id
                    JOIN leads l ON o.lead_id = l.id
                    join users u on o.current_sale_id=u.id
                    where o.current_sale_order_level_id >=16 
                      and t.t_status =1 
                      and t.payment_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
                      and u.role=3
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOT.csv",index=False)