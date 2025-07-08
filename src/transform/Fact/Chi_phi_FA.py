import pandas as pd
import os
from src.Get_data_DB import DataTransformer

transformer=DataTransformer()

# Lấy số FA trên ME
Query_FA= """ SELECT DATE(DATE_ADD(lp.created_at, INTERVAL 7 HOUR)) AS Date, 
                      p.code AS Ten_khoa_hoc,
                      p.id AS Ma_khoa_hoc,
                      COUNT(*) AS So_FA
                FROM leads l
                JOIN leads_products lp ON lp.lead_id = l.id
                JOIN products p ON lp.product_id = p.id
                WHERE l.utm_source = 'FA'
                  AND DATE(DATE_ADD(lp.created_at, INTERVAL 7 HOUR)) >= 'DATE_SUB(NOW(), INTERVAL 3 MONTH)'
                GROUP BY Date, Ten_khoa_hoc, Ma_khoa_hoc;
                """
Query_KH=""" select Ma_khoa_hoc, Ten_khoa_hoc 
            from Dim_Khoa_hoc
"""
# Đọc dữ liệu
df_count_FA=transformer.fetch_from_mysql(Query_FA)
df_KH=transformer.fetch_from_sql_server(Query_KH)

# Định nghĩa cấu trúc DataFrame mẫu khi file rỗng
empty_df_template = pd.DataFrame(columns=['Campaign ID', 'Campaign Name', 'Date', 'Spend'])

# Hàm kiểm tra và đọc file CSV
def read_csv_safe(file_path):
    full_path = os.path.expanduser(file_path)
    
    # Kiểm tra file có tồn tại và có kích thước > 0 byte không
    if not os.path.exists(full_path) or os.path.getsize(full_path) == 0:
        print(f"File {file_path} rỗng hoặc không tồn tại. Tạo DataFrame rỗng.")
        return empty_df_template.copy()
    
    try:
        # Thử đọc file CSV
        df = pd.read_csv(full_path)
        
        # Kiểm tra nếu DataFrame đọc được có dữ liệu
        if df.empty:
            print(f"File {file_path} không có dữ liệu. Tạo DataFrame rỗng.")
            return empty_df_template.copy()
            
        return df
    
    except pd.errors.EmptyDataError:
        print(f"File {file_path} không có dữ liệu (EmptyDataError). Tạo DataFrame rỗng.")
        return empty_df_template.copy()
    except Exception as e:
        print(f"Lỗi khi đọc file {file_path}: {str(e)}. Tạo DataFrame rỗng.")
        return empty_df_template.copy()

# Đường dẫn file
file_path1 = "~/DWH_Cole_Project/data_tmp/spend_C9_ACTIVE.csv"
file_path2 = "~/DWH_Cole_Project/data_tmp/spend_Cole8_ACTIVE.csv"

# Đọc dữ liệu
df_cf1 = read_csv_safe(file_path1)
df_cf2 = read_csv_safe(file_path2)

# Kiểm tra nếu một trong hai DataFrame rỗng
if df_cf1.empty and not df_cf2.empty:
    df_cf = df_cf2.copy()
    df_cf['Ten_khoa_hoc'] = df_cf['Campaign Name'].str.split('_').str[1]
elif df_cf2.empty and not df_cf1.empty:
    df_cf = df_cf1.copy()
    df_cf['Ten_khoa_hoc'] = df_cf['Campaign Name'].str.split('_').str[1]
else:
    # Chỉ thực hiện chuẩn hóa và nối nếu cả hai DataFrame đều không rỗng
    # Các bước chuẩn hoá dữ liệu 
    df_cf1['Ten_khoa_hoc'] = df_cf1['Campaign Name'].str.split('_').str[1]
    df_cf2['Ten_khoa_hoc'] = df_cf2['Campaign Name'].str.split('_').str[1]
    
    # Gộp dữ liệu chạy chuyển đổi của 2 tài khoản
    df_cf = pd.concat([df_cf1, df_cf2], ignore_index=True)


# Xử lí tên khoá học ghi chưa chuẩn trên Meta
df_cf['Ten_khoa_hoc'] = df_cf['Ten_khoa_hoc'].str.upper()

# Thay thế giá trị 'BI' bằng 'BI.01'
df_cf['Ten_khoa_hoc'] = df_cf['Ten_khoa_hoc'].replace('BI', 'BI.01', regex=False)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_count_FA["Date"] = pd.to_datetime(df_count_FA["Date"]).dt.date
df_cf["Date"] = pd.to_datetime(df_cf["Date"]).dt.date

# Lấy những bản ghi thoả mãn và bản ghi có chi phí nhưng không có số chuyển đổi --> Đổ vào bảng Chi_phi_FA
df_FA = df_cf.merge(df_count_FA, on=['Ten_khoa_hoc', 'Date'], how='left')
df_FA=df_FA[['Spend','So_FA','Date','Ten_khoa_hoc']]
df_FA['So_FA'] = df_FA['So_FA'].fillna(0)
df_FA = df_FA.rename(columns={
    'Spend': 'Chi_phi',
    'Date': 'Thoi_gian'
})
df_FA=df_FA.merge(df_KH,on='Ten_khoa_hoc',how='inner')
df_FA=df_FA[['Chi_phi','So_FA','Thoi_gian','Ma_khoa_hoc']]
df_FA = df_FA.groupby(['Thoi_gian', 'Ma_khoa_hoc'])[['Chi_phi', 'So_FA']].sum().reset_index()

# Trường hợp có số FA nhưng không có chi phí thì chuyển thành số L1_mess ở bảng Chi_phi_mess
df_L1_mess_bonus=df_cf.merge(df_count_FA, on=['Ten_khoa_hoc', 'Date'], how='right')
df_L1_mess_bonus=df_L1_mess_bonus[['Date','Spend','Ma_khoa_hoc','So_FA']]
df_L1_mess_bonus = df_L1_mess_bonus.rename(columns={
    'Spend': 'Chi_phi',
    'Date': 'Thoi_gian',
    'So_FA': 'So_L1_mess'
})
df_L1_mess_bonus = df_L1_mess_bonus[df_L1_mess_bonus['Chi_phi'].isna()]
df_L1_mess_bonus['Chi_phi'] = df_L1_mess_bonus['Chi_phi'].fillna(0)
df_L1_mess_bonus = df_L1_mess_bonus.groupby(['Thoi_gian', 'Ma_khoa_hoc'])[['Chi_phi', 'So_L1_mess']].sum().reset_index()

# Ghi dữ liệu vào folder data_result
df_FA.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_FA_transformed.csv", index=False)
df_L1_mess_bonus.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_mess_bonus_transformed.csv", index=False)