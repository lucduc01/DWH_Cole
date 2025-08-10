import pandas as pd
import re

df=pd.read_csv("~/DWH_Cole_Project/data_tmp/product.csv")

# Xử lí để phân loại dựa vào tên sản phẩm 
def phan_loai_khoa_hoc(row):
    if row['Trang_thai'] == 1:
        return 'Ngừng bán'
    
    ten = str(row['Ten_khoa_hoc'])
    
    if '.' in ten:
        return ten.split('.')[0]
    
    # Tìm vị trí chữ số đầu tiên
    match = re.search(r'\d', ten)
    if match:
        index = match.start()
        before_digit = ten[:index]
        if ' ' in before_digit:
            return before_digit.rsplit(' ', 1)[0]
        else:
            return before_digit
    else:
        # Không có dấu chấm, không có số → trả về nguyên chuỗi
        return ten

df = df.rename(columns={'code': 'Ten_khoa_hoc',
                        'id': 'Ma_khoa_hoc',
                        'status':'Trang_thai'})
df.loc[df['Trang_thai'] == 1, 'Ten_khoa_hoc'] = 'Ngừng bán'
df['Ma_phan_loai'] = df.apply(phan_loai_khoa_hoc, axis=1)

# (Tùy chọn) Lưu kết quả nếu cần
df.to_csv("~/DWH_Cole_Project/data_result/product_transformed.csv", index=False)
df['Ma_phan_loai'].drop_duplicates().to_csv("~/DWH_Cole_Project/data_result/classify_transformed.csv", index=False)