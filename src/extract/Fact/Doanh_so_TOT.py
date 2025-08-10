from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

# Lấy dữ liệu lần đầu từ năm 2024 Mysql
mysql_query = """SELECT 
                      tp.id AS Id,
                      s.money AS Doanh_so,
                      DATE_ADD(t.payment_at, INTERVAL 7 HOUR) AS Ngay_chuyen_tien,
                      s.email as Khach_hang,
                      p.id AS Ma_khoa_hoc,
                      o.current_sale_id AS Ma_saler,
                      l.utm_medium AS Ma_marketer,
                      o.source AS Ma_kenh
    FROM transactions t
    JOIN transactions_products tp ON tp.p_transaction_id = t.id
    JOIN products p ON tp.product_id = p.id
    JOIN orders o ON t.order_id = o.id
    join students s on s.order_id=o.id
    JOIN leads l ON o.lead_id = l.id
    join users u on o.current_sale_id=u.id
    WHERE t.t_status = 1
      AND DATE_ADD(t.payment_at, INTERVAL 7 HOUR) >= '2024-01-01'
      And u.role=3
    GROUP BY tp.id, Ma_khoa_hoc, Ma_saler, Ma_marketer, Ma_kenh,Khach_hang,Doanh_so
                    """
df = transformer.fetch_from_mysql(mysql_query)
df.to_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOT.csv",index=False)