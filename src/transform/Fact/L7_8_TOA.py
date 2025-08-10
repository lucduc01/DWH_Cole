import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/L7_8_TOA.csv")

# Chuẩn hoá dữ liệu
standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV','ToUyen','DangHang','HuyenTrang']
)
df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])

group_cols = ['Ngay_mo_lop' ,'Ma_lop_hoc' , 'Ma_kenh' , 'Ma_marketer' ,'Ma_saler']

# Tính L7 theo nhóm
df['L7'] = df.groupby(group_cols)['Level_Id'].transform(lambda x: (x == 16).sum())

# Tính tổng số bản ghi trong nhóm
df['Total'] = df.groupby(group_cols)['Level_Id'].transform('count')

# L8 = Total - L7
df['L8'] = df['Total'] - df['L7']

# Xoá cột trung gian nếu muốn
df = df.drop(columns=['Total','Level_Id'])
df = df.drop_duplicates(subset=group_cols)
df['Ngay_mo_lop'] = pd.to_datetime(df['Ngay_mo_lop'])

# Ghi dữ liệu
df.to_csv("~/DWH_Cole_Project/data_result/L7_8_TOA_transformed.csv", index=False)