from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql  DATE_SUB(NOW(), INTERVAL 3 MONTH)
mysql_query = """SELECT DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) AS Thoi_gian,                                               
                        lp.product_id AS Ma_khoa_hoc,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 1 THEN l.id END) AS L1,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 1 THEN l.id END) - COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 3 THEN l.id END) AS L1_L1C,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 16 THEN l.id END) AS L7,
                        COUNT(DISTINCT CASE WHEN s2.sale_order_level_id = 19 THEN l.id END) AS L8
                FROM leads l 
                JOIN leads_products lp ON lp.lead_id = l.id
                JOIN orders o ON o.lead_id = l.id
                JOIN sale_order_histories s1 ON s1.order_id = o.id AND s1.sale_order_level_id = 1
                JOIN sale_order_histories s2 ON s2.order_id = o.id 
                WHERE l.status = 1 
                AND DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) >=  DATE_SUB(NOW(), INTERVAL 3 MONTH)
                AND l.utm_source='FA'
                GROUP BY Thoi_gian, Ma_khoa_hoc
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Count_L1_8_FA.csv",index=False)