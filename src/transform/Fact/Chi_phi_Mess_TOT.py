import pandas as pd
import os
from src.Get_data_DB import DataTransformer
import datetime
from src.Process_utm import ColumnStandardizer
from rapidfuzz import fuzz, process

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
            from Dim_Khoa_hoc
"""

# Đọc dữ liệu
df_KH=transformer.fetch_from_sql_server(Query_KH)

df_L1=pd.read_csv("~/DWH_Cole_Project/data_result/Count_L_of_Mess.csv")
df_cf=pd.read_csv("~/DWH_Cole_Project/data_tmp/Chi_phi&So_mess_TOT.csv")


df_cf['Ma_marketer'] = df_cf['Campaign Name'].str.split('_').str[2]

df_cf['Ma_marketer'] = standardizer.transform(df_cf['Ma_marketer'])

df_cf['Ten_khoa_hoc'] = df_cf['Campaign Name'].str.split('_').str[1]
df_cf['Ten_khoa_hoc'] = df_cf['Ten_khoa_hoc'].str.upper()

mask = (df_cf['Campaign Name'] == 'FA_CDSDN_BinhND_Mess_Page CĐS_10_06') 
df_cf.loc[mask, 'Ten_khoa_hoc'] = 'DTDN'

# Thay thế giá trị 'BI' bằng 'BI.01'
df_cf['Ten_khoa_hoc'] = df_cf['Ten_khoa_hoc'].replace('BI', 'BI.01', regex=False)

# Bước 2: Chuẩn hoá cột df_cf['Ten_khoa_hoc']
standard_list, standard_map = build_standard_list(df_KH['Ten_khoa_hoc'].drop_duplicates())
df_cf['Ten_khoa_hoc'] = standardize_course_column(
    df_cf['Ten_khoa_hoc'],
    standard_list,
    standard_map,
    threshold=70
)

# Đồng nhất kiểu dữ liệu ngày ở 2 dataframe 
df_cf["Date"] = pd.to_datetime(df_cf["Date"]).dt.date
df_L1["Thoi_gian"] = pd.to_datetime(df_L1["Thoi_gian"]).dt.date
df_cf = df_cf.rename(columns={
    'Spend': 'Chi_phi',
    'Date': 'Thoi_gian',
    'Conversations Started (7d)':'So_mess'
})

# Join để lấy mã khoá hoc
df_cf=df_cf.merge(df_KH,on='Ten_khoa_hoc',how='inner')
df_cf=df_cf[['Chi_phi','Thoi_gian','Ma_khoa_hoc','Ma_marketer','So_mess']]
df_cf = df_cf.groupby(['Thoi_gian', 'Ma_khoa_hoc','Ma_marketer'])[['Chi_phi','So_mess']].sum().reset_index()

# Join dữ liệu Chi_phí với Số L1
df_Mess=df_cf.merge(df_L1,on=['Thoi_gian','Ma_khoa_hoc'], how='outer')
df_Mess['Ma_marketer']=df_Mess['Ma_marketer'].fillna('Khác')
df_Mess = df_Mess.fillna(0)
df_Mess=df_Mess[['Chi_phi','So_mess','Thoi_gian','Ma_khoa_hoc','Ma_marketer','L1','L1_L1C','L7','L8']]
df_Mess["Thoi_gian"] = pd.to_datetime(df_Mess["Thoi_gian"]).dt.date
df_Mess=df_Mess[df_Mess['Thoi_gian'] >= datetime.date(2025, 1, 1)]

# Vì sản phẩm DE.COMBO01 có 2 mã khoá nên ở bước ánh xạ từ Ten_khoa_hoc thành Ma_khoa_hoc phát sinh thành 2 bản ghi Chi phí =
# 1. Tách dữ liệu cần xử lý và phần còn lại
df_to_process = df_Mess[df_Mess['Ma_khoa_hoc'].isin([515, 550])]
df_others = df_Mess[~df_Mess['Ma_khoa_hoc'].isin([515, 550])]

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
    df_processed = pd.DataFrame(columns=df_Mess.columns)

# 3. Ghép lại dữ liệu
df_Mess = pd.concat([df_processed, df_others], ignore_index=True)
df_L78=df_Mess[['Thoi_gian','Ma_khoa_hoc','Ma_marketer','L7','L8']]
df_Mess=df_Mess[['Chi_phi','So_mess','Thoi_gian','Ma_khoa_hoc','Ma_marketer','L1','L1_L1C']]
# Ghi dữ liệu
df_L78.to_csv("~/DWH_Cole_Project/data_result/L78_Mess_transformed.csv", index=False)
df_Mess.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_Mess_transformed.csv", index=False)