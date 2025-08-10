import pandas as pd
import os
from rapidfuzz import fuzz, process
from src.Get_data_DB import DataTransformer
from src.Process_utm import ColumnStandardizer


standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=[]
)
transformer=DataTransformer()

# Các hàm để chuẩn hoá tên khoá học trong tên chiến dịch
def build_standard_list(series):
    """Tạo danh sách chuẩn từ Series: loại bỏ trùng và chuẩn hoá chữ thường"""
    clean_series = (
        series.dropna()
        .drop_duplicates()
        .astype(str)
        .str.strip()
    )
    standard_list = clean_series.str.lower().tolist()
    standard_map = dict(zip(clean_series.str.lower(), clean_series))
    return standard_list, standard_map

def match_course_name(value, standard_list, standard_map, threshold=60):
    """Tìm match fuzzy cho 1 tên khoá học"""
    if pd.isna(value):
        return "Khác"

    val = str(value).strip().lower()
    result = process.extractOne(val, standard_list, scorer=fuzz.ratio)

    if result is None:
        return "Khác"

    match, score, _ = result
    if score >= threshold:
        return standard_map[match]
    else:
        return "Khác"

def standardize_course_column(input_series, standard_list, standard_map, threshold=60):
    """Áp dụng chuẩn hoá cho cả cột"""
    return input_series.apply(lambda x: match_course_name(x, standard_list, standard_map, threshold))

Query_KH=""" select Ma_khoa_hoc, Ten_khoa_hoc 
            from Dim_Khoa_hoc """

# Đọc dữ liệu
df_KH=transformer.fetch_from_sql_server(Query_KH)

df_L=pd.read_csv("~/DWH_Cole_Project/data_tmp/Count_L1_8_FA.csv")
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

file_path3 = "~/DWH_Cole_Project/data_tmp/spend_C9_PAUSED.csv"   #Lấy dữ liệu 1 lần duy nhất ở lần chạy đầu tiên
file_path4 = "~/DWH_Cole_Project/data_tmp/spend_Cole8_PAUSED.csv"
file_path1 = "~/DWH_Cole_Project/data_tmp/spend_C9_ACTIVE.csv"
file_path2 = "~/DWH_Cole_Project/data_tmp/spend_Cole8_ACTIVE.csv"

# Đọc dữ liệu
df_cf1 = read_csv_safe(file_path1)
df_cf2 = read_csv_safe(file_path2)
df_cf3=pd.read_csv(file_path3)
df_cf4=pd.read_csv(file_path4)
df_cf = pd.concat([df_cf1,df_cf2,df_cf3,df_cf4], ignore_index=True)

df_cf['Ten_khoa_hoc'] = df_cf['Campaign Name'].str.split('_').str[1]
df_cf['Ma_marketer'] = df_cf['Campaign Name'].str.split('_').str[2]

df_cf['Ma_marketer'] = standardizer.transform(df_cf['Ma_marketer'])


# Bước 1: Tạo danh sách chuẩn
standard_list, standard_map = build_standard_list(df_KH['Ten_khoa_hoc'].drop_duplicates())

# Trường hợp đặc biệt 6 tháng đầu năm 2025, Ai.Nocode chạy cho khoá DTDN
df_cf['Date'] = pd.to_datetime(df_cf['Date'])
mask = (df_cf['Ten_khoa_hoc'] == 'Ai.Nocode') & (df_cf['Date'] >= '2025-01-01') & (df_cf['Date'] <= '2025-10-01')
df_cf.loc[mask, 'Ten_khoa_hoc'] = 'DTDN'   

df_cf['Ten_khoa_hoc'] = df_cf['Ten_khoa_hoc'].replace('BI', 'BI.01', regex=False)

# Bước 2: Chuẩn hoá cột df_cf['Ten_khoa_hoc']
df_cf['Ten_khoa_hoc'] = standardize_course_column(
    df_cf['Ten_khoa_hoc'],
    standard_list,
    standard_map,
    threshold=70
)

df_cf["Date"] = pd.to_datetime(df_cf["Date"]).dt.date
df_L["Thoi_gian"] = pd.to_datetime(df_L["Thoi_gian"]).dt.date
df_cf = df_cf.rename(columns={
    'Spend': 'Chi_phi',
    'Date': 'Thoi_gian'
})
df_cf=df_cf.merge(df_KH,on='Ten_khoa_hoc',how='inner')
df_cf = df_cf.drop(columns=['Campaign ID', 'Campaign Name','Ten_khoa_hoc'])


# Lấy những bản ghi thoả mãn và bản ghi có chi phí nhưng không có số chuyển đổi --> Đổ vào bảng Chi_phi_FA
df_cf = df_cf.groupby(['Thoi_gian', 'Ma_khoa_hoc','Ma_marketer'])[['Chi_phi']].sum().reset_index()

df_FA=df_cf.merge(df_L,on=['Thoi_gian','Ma_khoa_hoc'], how='left')

# Vì sản phẩm DE.COMBO01 có 2 mã khoá nên ở bước ánh xạ từ Ten_khoa_hoc thành Ma_khoa_hoc phát sinh thành 2 bản ghi Chi phí =
# 1. Tách dữ liệu cần xử lý và phần còn lại
df_to_process = df_FA[df_FA['Ma_khoa_hoc'].isin([515, 550])]
df_others = df_FA[~df_FA['Ma_khoa_hoc'].isin([515, 550])]

# 2. Nếu có bản ghi cần xử lý
if not df_to_process.empty:
    def xu_ly_nhom(gr):
        if len(gr) == 2:
            na_mask = gr['L1'].isna()
            if na_mask.sum() == 1:
                return gr[~na_mask]
            elif na_mask.sum() == 2:
                if (gr['Ma_khoa_hoc'] == 515).any():
                    return gr[gr['Ma_khoa_hoc'] == 515]
                else:
                    return pd.DataFrame()
            else:
                return gr
        else:
            return gr[~gr['L1'].isna()] if gr['L1'].isna().any() else gr

    df_processed = (
        df_to_process
        .groupby(['Chi_phi', 'Thoi_gian'], group_keys=False)
        .apply(xu_ly_nhom)
    )
else:
    # Không có gì để xử lý
    df_processed = pd.DataFrame(columns=df_FA.columns)

# 3. Ghép lại dữ liệu
df_FA = pd.concat([df_processed, df_others], ignore_index=True)


df_FA = df_FA.fillna(0)
df_L78=df_FA[['Thoi_gian','Ma_khoa_hoc','Ma_marketer','L7','L8']]
df_FA=df_FA[['Thoi_gian','Ma_khoa_hoc','Ma_marketer','Chi_phi','L1','L1_L1C']]

# Trường hợp có số L1 chuyển đổi  nhưng không có chi phí thì chuyển thành số L1_mess ở bảng Chi_phi_mess 
df_mess_bonus=df_cf.merge(df_L, on=['Ma_khoa_hoc', 'Thoi_gian'], how='right')
df_mess_bonus=df_mess_bonus[['Thoi_gian','Chi_phi','Ma_khoa_hoc','Ma_marketer','L1','L1_L1C','L7','L8']]
df_mess_bonus = df_mess_bonus[df_mess_bonus['Chi_phi'].isna()]
df_mess_bonus['Chi_phi'] = df_mess_bonus['Chi_phi'].fillna(0)


# Ghi dữ liệu vào folder data_result
df_FA.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_FA_transformed.csv", index=False)
df_L78.to_csv("~/DWH_Cole_Project/data_result/L78_FA_transformed.csv", index=False)
df_mess_bonus.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_mess_bonus_transformed.csv", index=False)