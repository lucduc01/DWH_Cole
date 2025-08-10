import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Doanh_so_TOT.csv")

#Chuẩn hoá dữ liệu
def process_grouped_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(0)
    df['Ngay_chuyen_tien'] = pd.to_datetime(df['Ngay_chuyen_tien'])

    # Nhóm theo 'Khach_hang' và 'Ma_khoa_hoc'
    def get_first_sorted(group):
        if len(group) > 1:
            group = group.sort_values('Ngay_chuyen_tien')
        return group.iloc[[0]]  # chỉ lấy bản ghi đầu tiên
    
    # Áp dụng cho từng nhóm
    df = df.groupby(['Khach_hang', 'Ma_khoa_hoc'], group_keys=False).apply(get_first_sorted)

    # 2. DataFrame 1A: Bỏ 3 cột
    df = df.drop(columns=['Khach_hang'])

    # 4. Gộp theo các cột còn lại (trừ Doanh_so)
    group_cols = [col for col in df.columns if col != 'Doanh_so']
    df_sum = df.groupby(group_cols, dropna=False)['Doanh_so'].sum().reset_index()

    return df_sum

standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV','ToUyen','DangHang','HuyenTrang']
)
df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])
df_result = process_grouped_sales(df)

df_result.to_csv("~/DWH_Cole_Project/data_result/Doanh_so_TOT_transformed.csv", index=False)