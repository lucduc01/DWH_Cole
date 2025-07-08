import pandas as pd
from src.Process_utm import ColumnStandardizer

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Doanh_thu_TOT.csv")
df_saler=pd.read_csv("~/DWH_Cole_Project/data_result/saler_transformed.csv")
# Chuẩn hoá dữ liệu
standardizer = ColumnStandardizer(
    threshold=75,
    preserve_if_low_similarity=['QuanNV']
)


df['Ma_marketer'] = standardizer.transform(df['Ma_marketer'])
df['Ngay_chuyen_tien'] = pd.to_datetime(df['Ngay_chuyen_tien'])

df.to_csv("~/DWH_Cole_Project/data_result/Doanh_thu_TOT_transformed.csv", index=False)