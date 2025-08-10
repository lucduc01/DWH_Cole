import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOA.csv")
# Chuẩn hoá dữ liệu
standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV','ToUyen','DangHang','HuyenTrang']
)

df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])
df['Ngay_mo_lop'] = pd.to_datetime(df['Ngay_mo_lop'])

# Tách thành 2 dataframe con cho 2 trường hợp sản phẩm là combo và không combo
df['Ten_san_pham_lower'] = df['Ten_san_pham'].str.lower()

# Tạo mask kiểm tra chứa chuỗi "combo"
mask_combo = df['Ten_san_pham_lower'].str.contains('combo', na=False)

# Tách DataFrame
df_combo = df[mask_combo].copy()
df_khong_combo = df[~mask_combo].copy()

# (Tùy chọn) Xoá cột tạm nếu không cần
df_combo.drop(columns='Ten_san_pham_lower', inplace=True)
df_khong_combo.drop(columns='Ten_san_pham_lower', inplace=True)

# Tính tổng Doanh_so 
df_khong_combo = df_khong_combo.drop(columns=['Ten_hoc_sinh','Ten_san_pham'])
df_khong_combo = df_khong_combo.groupby(
    [col for col in df_khong_combo.columns if col != 'Doanh_so']
).agg({'Doanh_so': 'sum'}).reset_index()

# Xử lí riêng cho trường hợp sản phẩm là COMBO, tính tiền phân về các lớp
def xu_ly_nhom(gr):
    # Nếu chỉ có 1 bản ghi thì không cần xử lý
    if len(gr) == 1:
        return gr

    # Kiểm tra xem các giá trị Doanh_so có bằng nhau không
    if gr['Doanh_so'].nunique() == 1:
        # Nếu bằng nhau: chia giá trị đó cho số bản ghi
        gia_tri_moi = gr['Doanh_so'].iloc[0] / len(gr)
    else:
        # Nếu khác nhau: cộng tổng và chia đều
        gia_tri_moi = gr['Doanh_so'].sum() / len(gr)

    # Gán lại cột Doanh_so bằng giá trị mới
    gr['Doanh_so'] = gia_tri_moi
    return gr

# Áp dụng hàm cho từng nhóm theo Ten_hoc_sinh
df_combo = df_combo.groupby(['Ten_hoc_sinh', 'Ten_san_pham'], group_keys=False).apply(xu_ly_nhom)
df_combo = df_combo.drop(columns=['Ten_hoc_sinh','Ten_san_pham'])
df_combo = df_combo.groupby(
    [col for col in df_combo.columns if col != 'Doanh_so']
).agg({'Doanh_so': 'sum'}).reset_index()

df=pd.concat([df_khong_combo, df_combo], ignore_index=True)
df = df.groupby(
    [col for col in df_combo.columns if col != 'Doanh_so']
).agg({'Doanh_so': 'sum'}).reset_index()

# Ghi dữ liệu đã xử lí
df.to_csv("~/DWH_Cole_Project/data_result/Doanh_so_TOA_transformed.csv",index=False)
