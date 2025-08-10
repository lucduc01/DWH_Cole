from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql  DATE_SUB(NOW(), INTERVAL 3 MONTH)
mysql_query = """SELECT DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) AS Ngay,                                               
                        l.source AS Ma_kenh, 
                        l.utm_medium AS Ma_marketer,
                        lp.product_id AS Ma_khoa_hoc,
                        o.current_sale_id AS Ma_saler,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 1 THEN l.id END) AS L1,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 2 THEN l.id END) AS L1B,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 1 THEN l.id END) - COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 3 THEN l.id END) AS L1_L1C,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 4 THEN l.id END) AS L2,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 7 THEN l.id END) AS L3,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 13 THEN l.id END) AS L6,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 16 THEN l.id END) AS L7,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 19 THEN l.id END) AS L8
                FROM leads l 
                JOIN leads_products lp ON lp.lead_id = l.id
                JOIN orders o ON o.lead_id = l.id
                JOIN users u ON o.current_sale_id = u.id 
                JOIN sale_order_histories s1 ON s1.order_id = o.id AND s1.sale_order_level_id = 1
                JOIN sale_order_histories s2 ON s2.order_id = o.id 
                WHERE l.status = 1 
                AND DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) >= '2024-01-01'
                AND u.role = 3 
                GROUP BY Ngay, Ma_kenh, Ma_marketer, Ma_khoa_hoc, Ma_saler
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Count_Level_TOT.csv",index=False)