import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Count_Level_TOT.csv")

# Chuẩn hoá dữ liệu
standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV','ToUyen','DangHang','HuyenTrang']
)

df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])
# Tính tổng các cột L theo các cột Ngay, Ma_kenh, Ma_marketer, Ma_khoa_hoc, Ma_saler
df = df.groupby(['Ngay', 'Ma_kenh', 'Ma_marketer', 'Ma_khoa_hoc', 'Ma_saler']).agg(
    L1=('L1', 'sum'),
    L1B=('L1B', 'sum'),
    L1_L1C=('L1_L1C', 'sum'),
    L2=('L2', 'sum'),
    L3=('L3', 'sum'),
    L6=('L6', 'sum'),
    L7=('L7', 'sum'),
    L8=('L8', 'sum')
).reset_index()

df['Ngay'] = pd.to_datetime(df['Ngay'])
df.to_csv("~/DWH_Cole_Project/data_result/Count_Level_TOT_transformed.csv", index=False)