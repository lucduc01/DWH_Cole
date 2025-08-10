import pandas as pd
from src.Get_data_DB import DataTransformer
from rapidfuzz import fuzz, process

transform=DataTransformer()

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Marketing_plan.csv")
Product_query='select Ma_khoa_hoc, Ten_khoa_hoc from Dim_Khoa_hoc where Ma_khoa_hoc != 550'

df_product=transform.fetch_from_sql_server(Product_query)

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

# Bước 2: Chuẩn hoá cột df_cf['Ten_khoa_hoc']
df['Khoá học'] = standardize_course_column(
    df['Khoá học'],
    standard_list,
    standard_map,
    threshold=75
)
# Điền dữ liệu thiếu ở các cột Năm, Tháng
df[['Năm', 'Tháng']] = df[['Năm', 'Tháng']].ffill()
df['Năm'] = df['Năm'].astype(int)
df['Tháng'] = df['Tháng'].astype(int)

cols_to_fill_zero = df.columns.difference(['Năm', 'Tháng','Khoá học'])
df[cols_to_fill_zero] = df[cols_to_fill_zero].fillna(0)

# Ánh xạ để lấy Ma_khoa_hoc từ Ten_khoa_hoc
df=df.merge(df_product,left_on='Khoá học', right_on='Ten_khoa_hoc', how='inner')
df=df.drop(columns=['Ten_khoa_hoc','Khoá học'])

df=df.rename(columns={
    'Năm': 'Nam',
    'Tháng': 'Thang',
    'Chi phí ': 'Chi_phi'
})

# Tạo dataframe 1 để lưu trữ  dữ liệu kế hoạch theo Tháng
df1 = df[['Nam', 'Thang', 'L1', 'Chi_phi', 'Ma_khoa_hoc']]
df1.loc[:, 'Chi_phi'] = df1['Chi_phi'].astype(str).str.replace(r'[.,]', '', regex=True)

# DataFrame 2: Xử lý tuần
# Tách 2 phần Tuần L1 và Tuần Chi phí
df_l1_weeks = df[['Nam', 'Thang', 'Ma_khoa_hoc', 'Tuần 1', 'Tuần 2', 'Tuần 3', 'Tuần 4', 'Tuần 5']].copy()
df_cost_weeks = df[['Tuần 1.1', 'Tuần 2.1', 'Tuần 3.1', 'Tuần 4.1', 'Tuần 5.1']].copy()

# Đổi tên cột tuần chi phí để khớp theo tuần
df_cost_weeks.columns = ['Tuần 1', 'Tuần 2', 'Tuần 3', 'Tuần 4', 'Tuần 5']

# Melt tuần L1
df_l1_melt = df_l1_weeks.melt(id_vars=['Nam', 'Thang', 'Ma_khoa_hoc'], 
                              var_name='Tuần', 
                              value_name='L1')

# Melt tuần Chi phí
df_cost_melt = df_cost_weeks.melt(var_name='Tuần', value_name='Chi_phi')
df_cost_melt.loc[:, 'Chi_phi'] = df_cost_melt['Chi_phi'].astype(str).str.replace(r'[.,]', '', regex=True)

# Ghép lại 2 phần
df2 = df_l1_melt.copy()
df2['Chi_phi'] = df_cost_melt['Chi_phi']

# Sắp xếp nếu cần
df2 = df2[['Nam', 'Thang', 'Ma_khoa_hoc', 'Tuần', 'L1', 'Chi_phi']]
df2=df2.rename(columns={
    'Tuần': 'Tuan'
})
df2['Tuan'] = df2['Tuan'].str.extract(r'(\d+)')
df2['L1'] = df2['L1'].astype('int')

# Ghi dữ liệu vào folder data_result
df1.to_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Marketing_Thang_transformed.csv", index=False)
df2.to_csv("~/DWH_Cole_Project/data_result/Ke_hoach_Marketing_Tuan_transformed.csv", index=False)