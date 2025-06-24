import pandas as pd

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/classify.csv")
# 1. Thay thế giá trị NaN thành "Cole chưa điền"
df= df.fillna("Cole chưa điền")

# 2. Loại bỏ các dòng trùng lặp (giữ lại bản ghi đầu tiên)
df= df.drop_duplicates()
df = df.rename(columns={'classify': 'Ma_phan_loai'})

# (Tùy chọn) Lưu kết quả nếu cần
df.to_csv("~/DWH_Cole_Project/data_result/classify_transformed.csv", index=False)