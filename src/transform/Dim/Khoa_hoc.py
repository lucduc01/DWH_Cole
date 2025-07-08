import pandas as pd

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/product.csv")
# 1. Thay thế giá trị NaN thành "Cole chưa điền"
df['classify']= df['classify'].fillna("Cole chưa điền")

# 2. Loại bỏ các dòng trùng lặp (giữ lại bản ghi đầu tiên)
df= df.drop_duplicates()
df = df.rename(columns={'classify': 'Ma_phan_loai',
                        'code': 'Ten_khoa_hoc',
                        'id': 'Ma_khoa_hoc',
                        'status':'Trang_thai'})
df.loc[df['Trang_thai'] == 1, 'Ten_khoa_hoc'] = 'Ngừng bán'

# (Tùy chọn) Lưu kết quả nếu cần

df.to_csv("~/DWH_Cole_Project/data_result/product_transformed.csv", index=False)