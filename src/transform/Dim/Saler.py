import pandas as pd

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/saler.csv")

# 1. Giữ lại tên Nhân viên đang làm việc 
df= df.fillna("Đã nghỉ")
df.loc[df['Trang_thai'] == 1, 'Ten_nhan_vien'] = 'Đã nghỉ'

df.to_csv("~/DWH_Cole_Project/data_result/saler_transformed.csv", index=False)