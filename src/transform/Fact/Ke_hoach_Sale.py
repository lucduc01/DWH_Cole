import pandas as pd
from src.Get_data_DB import DataTransformer
from rapidfuzz import fuzz, process
import math

transform=DataTransformer()

# Lấy các dữ liệu cần thiết

Product_query='select Ma_khoa_hoc, Ten_khoa_hoc from Dim_Khoa_hoc where Ma_khoa_hoc != 550'
Class_query='select Ma_lop_hoc, Ten_lop_hoc from Dim_Lop_hoc'

df_product=transform.fetch_from_sql_server(Product_query)
df_class=transform.fetch_from_sql_server(Class_query)

df1=pd.read_csv("~/DWH_Cole_Project/data_tmp/Ke_hoach_Sale_TOA.csv")
df2=pd.read_csv("~/DWH_Cole_Project/data_tmp/Ke_hoach_Sale_TOT.csv")

# Hàm làm tròn số thập phân
def custom_round(x):
    return math.floor(x) if x - math.floor(x) < 0.5 else math.ceil(x)

# Các hàm để đảm bảo tên khoá học chuẩn trong file kế hoạch
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

# Bước 1: Tạo danh sách chuẩn
standard_list, standard_map = build_standard_list(df_product['Ten_khoa_hoc'].drop_duplicates())

#--- Xử lí dữ liệu Kế hoạch TOA----
df1=df1.rename(columns={
    'Dữ liệu kế hoạch theo tháng':'Nam',
    'Unnamed: 1':'Thang',
    'Unnamed: 2':'Ten_khoa_hoc',
    'Unnamed: 3':'Thu_tu_lop',
    'Unnamed: 4':'L1',
    'Unnamed: 5':'L1_L1C_Sale',
    'Unnamed: 6':'L1_L1C',
    'Unnamed: 7':'L2',
    'Unnamed: 8':'Doanh_so',
    'Unnamed: 9':'Doanh_so_Sale'
})

# Xử lí 2 cột Năm, Tháng
df1['Nam']=df1['Nam'].ffill()
df1['Thang']=df1['Thang'].ffill()
df1['Thang'] = df1['Thang'].str.extract(r'(\d+)')[0].astype(int)

# Xử lí dữ liệu để lấy được tên lớp chuẩn
df1['Ten_khoa_hoc'] = df1['Ten_khoa_hoc'].str.replace('\n', '', regex=False)
df1['Ten_khoa_hoc'] = df1['Ten_khoa_hoc'].replace(['Khóa DS, DA ','Khóa DS, DA'], 'DS.01')
# In hoa cột
df1['Thu_tu_lop'] = df1['Thu_tu_lop'].str.upper()

# Trích xuất mã K
df1['Thu_tu_lop'] = df1['Thu_tu_lop'].str.extract(r'(K\d+)')[0]

df1 = df1.dropna(subset=['Thu_tu_lop'])

# Tìm mã K có độ dài = 2 và thêm số 0
mask = (df1['Thu_tu_lop'].str.len() == 2) & df1['Thu_tu_lop'].notna()
df1.loc[mask, 'Thu_tu_lop'] = df1.loc[mask, 'Thu_tu_lop'].str.replace(r'K(\d)', r'K0\1', regex=True)

# Bước 2: Chuẩn hoá cột df_cf['Ten_khoa_hoc']
df1['Ten_khoa_hoc'] = standardize_course_column(
    df1['Ten_khoa_hoc'],
    standard_list,
    standard_map,
    threshold=75
)

# Tạo cột Ten_lop_hoc
df1['Ten_lop_hoc'] = df1['Ten_khoa_hoc'].astype(str) + '-' + df1['Thu_tu_lop'].astype(str)
df1=df1.merge(df_class,on='Ten_lop_hoc', how='inner')

pattern = r'[.đ\s]'  # Loại bỏ dấu chấm, chữ đ và khoảng trắng
df1['Doanh_so'] = df1['Doanh_so'].str.replace(pattern, '', regex=True)
df1['Doanh_so_Sale'] = df1['Doanh_so_Sale'].str.replace(pattern, '', regex=True)

# Tạo cột Thoi_gian dạng YYYY-MM-DD 
df1['Thoi_gian'] = pd.to_datetime(
    df1.rename(columns={'Nam': 'year', 'Thang': 'month'})[['year', 'month']]
    .assign(day=1)
)
df1=df1.drop(columns=['Nam','Thang','Ten_khoa_hoc','Thu_tu_lop','Ten_lop_hoc'])

#--- Xử lí dữ liệu Kế hoạch TOT ----
df2=df2.rename(columns={
    'Năm':'Nam',
    'Tháng':'Thang',
    'Khoá học': 'Ten_khoa_hoc',
    'L1-L1C':'L1_L1C',
    'L1-L1C/Sale':'L1_L1C_Sale',
    'L7+L8':'L7_8',
    '(L7+L8)/Sale':'L7_8_Sale',
    'Doanh số': 'Doanh_so',
    'Doanh số/Sale':'Doanh_so_Sale'
})

# Điền giá trị thiếu ở 2 cột Thời gian
df2['Nam']=df2['Nam'].ffill()
df2['Thang']=df2['Thang'].ffill()

# Tại thời điểm ngày 11/8/2025. Cole kinh doanh sản phẩm cho mua gộp DS và DA , team Sale quy hết về DS.01
df2['Ten_khoa_hoc'] = df2['Ten_khoa_hoc'].replace(['DS, DA ','DS, DA'], 'DS.01')

# Chuẩn số giá trị ở 2 cột Doanh số
pattern = r'[.đ\s]'  # Loại bỏ dấu chấm, chữ đ và khoảng trắng
df2['Doanh_so'] = df2['Doanh_so'].str.replace(pattern, '', regex=True)
df2['Doanh_so_Sale'] = df2['Doanh_so_Sale'].str.replace(pattern, '', regex=True)

# Chuẩn hoá giá trị cột L7+L8/Sale
df2['L7_8_Sale']=df2['L7_8_Sale'].str.replace(',', '.', regex=False).astype(float)
df2=df2.fillna(0)
df2['L7_8_Sale']=df2['L7_8_Sale'].apply(custom_round)

# Bước 2: Chuẩn hoá cột df_cf['Ten_khoa_hoc']
df2['Ten_khoa_hoc'] = standardize_course_column(
    df2['Ten_khoa_hoc'],
    standard_list,
    standard_map,
    threshold=75
)

# Ánh xạ Ten_khoa_hoc -> Ma_khoa_hoc
df2=df2.merge(df_product,on='Ten_khoa_hoc',how='inner').drop(columns='Ten_khoa_hoc')

# Tạo cột Thoi_gian dạng YYYY-MM-DD 
df2['Thoi_gian'] = pd.to_datetime(
    df2.rename(columns={'Nam': 'year', 'Thang': 'month'})[['year', 'month']]
    .assign(day=1)
)
df2=df2.drop(columns=['Nam','Thang'])

# Ghi dữ liệu 
df1.to_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Sale_TOA.csv",index=False)
df2.to_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Sale_TOT.csv",index=False)