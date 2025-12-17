import pandas as pd
from src.Get_data_DB import DataTransformer
from dotenv import load_dotenv
import os
from datetime import datetime

#------ Vì dữ liệu ngày ở Leads lên chậm hơn so với thực tế ( Google Sheet), lấy dữ liệu Ngày ở Sheet ghép với Id ở bảng Leads
pd.set_option('display.max_colwidth', None)
transformer=DataTransformer()
load_dotenv()

def read_public_google_sheet(sheet_url):
    """
    Đọc dữ liệu từ một Google Sheet công khai bằng cách chuyển đổi thành định dạng CSV.

    Args:
        sheet_url (str): URL của Google Sheet (ví dụ: https://docs.google.com/spreadsheets/d/123xyz/edit#gid=0).

    Returns:
        pandas.DataFrame: DataFrame chứa dữ liệu từ Google Sheet.
                          None nếu có lỗi xảy ra.
    """
    try:
        # Trích xuất ID của spreadsheet từ URL
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]

        # Trích xuất gid (ID của sheet/tab cụ thể) từ URL, nếu có
        # Mặc định là 0 nếu không có gid trong URL
        gid_part = sheet_url.split('#gid=')
        gid = gid_part[1] if len(gid_part) > 1 else '0'

        # Tạo URL để xuất file CSV
        csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'

        # Đọc dữ liệu trực tiếp vào DataFrame
        df = pd.read_csv(csv_url)
        print("Đọc dữ liệu thành công từ Google Sheet công khai!")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu từ Google Sheet công khai: {e}")
        print("Hãy đảm bảo Google Sheet đã được đặt ở chế độ 'Anyone with the link can view'.")
        return None

# --- Cách sử dụng ---
# Thay thế URL này bằng URL Google Sheet của bạn
# Đảm bảo Sheet của bạn đã được chia sẻ ở chế độ "Anyone with the link can view"
google_sheet_link = os.getenv("Sheet_L1_mess") 

query_Id_Lead_Mess= """
select l.id as Id_leads,
       l.name as Contact,
       l.mobile  as SDT,
       p.code as San_pham
from leads l
join leads_products lp on lp.lead_id =l.id
join products p on lp.product_id=p.id
where DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR))>='2025-01-01' 
    and l.utm_source ='mess'   """

df_leads=transformer.fetch_from_mysql(query_Id_Lead_Mess)
df_master_data = read_public_google_sheet(google_sheet_link)
df_master_data=df_master_data.dropna()


# --- Lấy tháng, năm hiện tại
today = datetime.today()
current_month = today.month
current_year = today.year

# --- Tính tháng trước
if current_month == 1:
    previous_month = 12
    previous_year = current_year - 1
else:
    previous_month = current_month - 1
    previous_year = current_year

# --- Đảm bảo cột Tháng và Năm trong df_master_data là số (nếu chưa)
df_master_data['Tháng'] = pd.to_numeric(df_master_data['Tháng'], errors='coerce')
df_master_data['Năm'] = pd.to_numeric(df_master_data['Năm'], errors='coerce')

# --- Lọc bản ghi theo điều kiện Tháng hiện tại và Tháng trước
df_master_data = df_master_data[
    ((df_master_data['Tháng'] == current_month) & (df_master_data['Năm'] == current_year)) |
    ((df_master_data['Tháng'] == previous_month) & (df_master_data['Năm'] == previous_year))
].copy()

# Danh sách để lưu trữ các DataFrame
dfs = []
# Duyệt qua từng link trong danh sách
for link in df_master_data['Link']:
    df_temp = read_public_google_sheet(link)  # Đọc dữ liệu từ link
    dfs.append(df_temp)  # Thêm DataFrame vừa đọc vào danh sách

# Kết hợp tất cả các DataFrame trong danh sách thành một DataFrame
df_Sheet = pd.concat(dfs, ignore_index=True)

df_Sheet['Ngày'] = df_Sheet['Ngày'].ffill()
df_Sheet['Ngày'] = pd.to_datetime(df_Sheet['Ngày'], dayfirst=True)

# Định dạng lại cột 'Ngày' theo định dạng "Năm-Tháng-Ngày"
df_Sheet['Ngày'] = df_Sheet['Ngày'].dt.strftime('%Y-%m-%d')

df_Sheet = df_Sheet.dropna(subset=['SĐT'])
df_Sheet['SĐT'] = df_Sheet['SĐT'].astype(str).apply(lambda x: '0' + x)
df_Sheet['SĐT'] = df_Sheet['SĐT'].str.replace('.0', '', regex=False)
df_Sheet['SĐT'] = df_Sheet['SĐT'].str.replace(r'^00', '0', regex=True)

df_leads=df_leads.rename(columns={
    'SDT' :'SĐT',
    'San_pham': 'Sản phẩm',
    'Id_leads': 'Ma_lead'
})
df_Sheet=df_Sheet[['Ngày','SĐT','Contact','Sản phẩm']]
df_merged=df_Sheet.merge(df_leads,on=['SĐT'], how='inner')
result = df_merged.loc[df_merged.groupby('SĐT')['Ma_lead'].idxmin()]
result=result[['Ngày','Ma_lead']]

#-----Lấy dữ liệu số L1, L1-L1.C, L7,L8 tương tự như FA nhưng chuyển số liệu ngày về khớp trong Sheet
# Lấy thông tin của L1.C
mysql_query1="""SELECT DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) AS Ngay,
                                 l.id as Ma_lead,
                                 s2.order_id,
                                 s2.sale_order_level_id,
                                 op.product_id AS Ma_khoa_hoc
                FROM leads l
                JOIN orders o ON o.lead_id = l.id
                JOIN sale_order_histories s1 ON s1.order_id = o.id AND s1.sale_order_level_id = 1
                JOIN sale_order_histories s2 ON s2.order_id = o.id AND s2.sale_order_level_id = 3
                JOIN orders_products op ON op.order_id = o.id
                WHERE l.utm_source ="mess"
                        and  l.status =1
                        and DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
                GROUP BY Ngay,Ma_lead,s2.order_id,s2.sale_order_level_id, Ma_khoa_hoc                      
"""
df1 = transformer.fetch_from_mysql(mysql_query1)

# Từ dataframe result, chuyển dữ liệu Ngày trong truy vấn về Ngày trong Sheet
df1=df1.merge(result, on='Ma_lead', how='inner')
df1=df1[['Ngày','Ma_lead','Ma_khoa_hoc']]

#Đếm số L1C
df1 = df1.groupby(['Ngày', 'Ma_khoa_hoc'])['Ma_lead'].nunique().reset_index()
df1.rename(columns={'Ma_lead': 'L1C', 'Ngày':'Thoi_gian'}, inplace=True)


#Lấy số L1
mysql_query2 = """select date(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) as Thoi_gian,
                        l.id as Ma_lead,
                        lp.product_id as Ma_khoa_hoc                  
                    from leads l 
                    join leads_products lp on lp.lead_id=l.id 
                    where date(DATE_ADD(l.created_at, INTERVAL 7 HOUR))>= DATE_SUB(NOW(), INTERVAL 24 MONTH)
                      and l.utm_source ="mess"
                      and l.status=1
                    group by Thoi_gian, Ma_lead, Ma_khoa_hoc
                    """
df2 = transformer.fetch_from_mysql(mysql_query2)
df2=df2.merge(result, on='Ma_lead', how='inner')
df2=df2[['Ngày','Ma_lead','Ma_khoa_hoc']]

# Đếm số L1
df2 = df2.groupby(['Ngày', 'Ma_khoa_hoc']).size().reset_index(name='L1')
df2.rename(columns={ 'Ngày':'Thoi_gian'}, inplace=True)


# Lấy số L7
mysql_query3="""SELECT DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) AS Ngay,
                                 l.id as Ma_lead,
                                 s2.order_id,
                                 s2.sale_order_level_id,
                                 op.product_id AS Ma_khoa_hoc
                FROM leads l
                JOIN orders o ON o.lead_id = l.id
                JOIN sale_order_histories s1 ON s1.order_id = o.id AND s1.sale_order_level_id = 1
                JOIN sale_order_histories s2 ON s2.order_id = o.id AND s2.sale_order_level_id = 16
                JOIN orders_products op ON op.order_id = o.id
                WHERE l.utm_source ="mess"
                        and  l.status =1
                        and DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
                GROUP BY Ngay,Ma_lead,s2.order_id,s2.sale_order_level_id, Ma_khoa_hoc                      
"""
df3 = transformer.fetch_from_mysql(mysql_query3)

# Từ dataframe result, chuyển dữ liệu Ngày trong truy vấn về Ngày trong Sheet
df3=df3.merge(result, on='Ma_lead', how='inner')
df3=df3[['Ngày','Ma_lead','Ma_khoa_hoc']]

#Đếm số L7
df3 = df3.groupby(['Ngày', 'Ma_khoa_hoc'])['Ma_lead'].nunique().reset_index()
df3.rename(columns={'Ma_lead': 'L7', 'Ngày':'Thoi_gian'}, inplace=True)

# Lấy số L8
mysql_query4="""SELECT DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) AS Ngay,
                                 l.id as Ma_lead,
                                 s2.order_id,
                                 s2.sale_order_level_id,
                                 op.product_id AS Ma_khoa_hoc
                FROM leads l
                JOIN orders o ON o.lead_id = l.id
                JOIN sale_order_histories s1 ON s1.order_id = o.id AND s1.sale_order_level_id = 1
                JOIN sale_order_histories s2 ON s2.order_id = o.id AND s2.sale_order_level_id = 19
                JOIN orders_products op ON op.order_id = o.id
                WHERE l.utm_source ="mess"
                        and  l.status =1
                        and DATE(DATE_ADD(l.created_at, INTERVAL 7 HOUR)) >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
                GROUP BY Ngay,Ma_lead,s2.order_id,s2.sale_order_level_id, Ma_khoa_hoc                      
"""
df4 = transformer.fetch_from_mysql(mysql_query4)

# Từ dataframe result, chuyển dữ liệu Ngày trong truy vấn về Ngày trong Sheet
df4=df4.merge(result, on='Ma_lead', how='inner')
df4=df4[['Ngày','Ma_lead','Ma_khoa_hoc']]

#Đếm số L8
df4 = df4.groupby(['Ngày', 'Ma_khoa_hoc'])['Ma_lead'].nunique().reset_index()
df4.rename(columns={'Ma_lead': 'L8', 'Ngày':'Thoi_gian'}, inplace=True)


# Ghi dữ liệu 
df1.to_csv("~/DWH_Cole_Project/data_tmp/So_L1C_Mess_TOT.csv",index=False)
df2.to_csv("~/DWH_Cole_Project/data_tmp/So_L1_Mess_TOT.csv",index=False)
df3.to_csv("~/DWH_Cole_Project/data_tmp/So_L7_Mess_TOT.csv",index=False)
df4.to_csv("~/DWH_Cole_Project/data_tmp/So_L8_Mess_TOT.csv",index=False)