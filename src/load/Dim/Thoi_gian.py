from Connect_DB import mysql_conn, mysql_cursor, sql_server_conn, sql_server_cursor
from datetime import datetime, timedelta
from calendar import monthrange

# Hàm để xác định tuần trong tháng
def get_week_in_month(date):
    first_day = date.replace(day=1)
    days_in_month = monthrange(date.year, date.month)[1]
    week = (date.day - 1) // 7 + 1
    return min(week, (days_in_month - 1) // 7 + 1)  # Đảm bảo không vượt quá số tuần thực tế

# Hàm để xác định quý
def get_quarter(month):
    return (month - 1) // 3 + 1

# Tạo dữ liệu cho bảng Dim_Thoi_gian
start_date = datetime(2014, 1, 1)
end_date = datetime(2040, 12, 31)
current_date = start_date

insert_query = """
INSERT INTO Dim_Thoi_gian (Ma_ngay_du, Ngay, Tuan, Thang, Quy, Nam)
VALUES (?, ?, ?, ?, ?, ?)
"""

data_to_insert = []
while current_date <= end_date:
    day = current_date.day
    month = current_date.month
    year = current_date.year
    week = get_week_in_month(current_date)
    quarter = get_quarter(month)
    
    data_to_insert.append((
        current_date.date(),
        day,
        week,
        month,
        quarter,
        year
    ))
    
    current_date += timedelta(days=1)

# Thực thi nhiều dòng dữ liệu cùng lúc
sql_server_cursor.executemany(insert_query, data_to_insert)

# Commit transaction
sql_server_conn.commit()

print(f"Inserted {len(data_to_insert)} rows into Dim_Thoi_gian.")