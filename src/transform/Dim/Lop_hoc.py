import pandas as pd
import re

# Đọc dữ liệu
df=pd.read_csv("~/DWH_Cole_Project/data_tmp/class.csv")

#Chuẩn hoá dữ liệu
def normalize_data(item):
    # Bước 1: Xóa khoảng trắng
    item = item.lstrip()
    
    # Bước 2: Chuyển -số thành -Ksố
    item = re.sub(r'-(\d+)', r'-K\1', item)
    
    # Bước 3: Chuẩn hóa số sau K thành 2 chữ số
    item = re.sub(r'K(\d{1,2})', lambda m: f'K{int(m.group(1)):02d}', item)
    
    # Bước 4: Xử lý ký tự trước K
    # Chuyển .K thành -K
    item = re.sub(r'\.K', '-K', item)
    # Chèn - vào giữa chữ cái và K (nếu trước K là chữ cái)
    item = re.sub(r'([a-zA-Z])K(?=\d)', r'\1-K', item)
    
    # Bước 5: Chuyển MODUL thành MODULE
    item = re.sub(r'MODUL(?![E])', 'MODULE', item, flags=re.IGNORECASE)
    item = item.replace("MODULE", "")
    
    # Bước 6: Chuyển BIG thành BD
    item = item.replace("BIG", "BD")

    # Bước 7: Xử lý chuỗi sau K+chữ số
    match = re.match(r'^(.*?)(-K\d{2})(\((.*?)\)|-(.*?))?$', item)
    if match:
        prefix, k_number, _, paren_content, suffix = match.groups()
        # Nếu chuỗi sau K+chữ số chứa ít nhất 2 chữ số, xóa chuỗi đó
        if paren_content and re.search(r'\d.*\d', paren_content):
            item = f"{prefix}{k_number}"
        elif suffix and re.search(r'\d.*\d', suffix):
            item = f"{prefix}{k_number}"
        # Nếu không, chuyển chuỗi sau lên trước -K+chữ số
        elif suffix:
            item = f"{prefix}-{suffix}{k_number}"
        elif paren_content:
            item = f"{prefix}-{paren_content}{k_number}"
    
    return item

def split_ten_lop_so_thu_tu(item):
    if re.search(r'-K\d{2}$', item):
        # Tách chuỗi trước -K và gán vào Ten_lop
        ten_lop = item.rsplit('-K', 1)[0]
        # Lấy 2 chữ số sau K và gán vào So_thu_tu
        so_thu_tu = int(item.rsplit('-K', 1)[1])
    else:
        # Nếu không chứa -K+số, gán So_thu_tu = 1 và Ten_lop = item
        ten_lop = item
        so_thu_tu = 1
    return pd.Series([ten_lop, so_thu_tu])

def calculate_so_ngay_tuyen_sinh(group):
    if len(group) == 1:
        group['So_buoi_tuyen_sinh'] = 0
        return group

    # Sắp xếp
    if group['Ngay_khai_giang'].isna().any():
        group = group.sort_values('So_thu_tu')
    else:
        group = group.sort_values('Ngay_khai_giang')

    # Reset index để xử lý dễ hơn (nhưng vẫn giữ lại thứ tự gốc)
    group = group.reset_index(drop=True)

    so_ngay_tuyen_sinh = [1]
    for i in range(1, len(group)):
        prev_date = group['Ngay_khai_giang'].iloc[i - 1]
        curr_date = group['Ngay_khai_giang'].iloc[i]
        if pd.isna(prev_date) or pd.isna(curr_date):
            so_ngay_tuyen_sinh.append(-1)
        else:
            so_ngay_tuyen_sinh.append((curr_date - prev_date).days)
    group['So_buoi_tuyen_sinh'] = so_ngay_tuyen_sinh

    return group


df['Ngay_khai_giang'] = pd.to_datetime(df['Ngay_khai_giang'])
df['Ngay_ket_thuc'] = pd.to_datetime(df['Ngay_ket_thuc'])

# Chuẩn hóa toàn bộ dữ liệu
df['Ten_lop_hoc'] = df['Ten_lop_hoc'].apply(normalize_data)
df[['Ten_lop', 'So_thu_tu']] = df['Ten_lop_hoc'].apply(split_ten_lop_so_thu_tu)

# Nhóm theo Ten_lop và áp dụng hàm calculate_so_ngay_tuyen_sinh
df = df.groupby('Ma_khoa_hoc', group_keys=False).apply(calculate_so_ngay_tuyen_sinh)

df=df[["Ma_lop_hoc","Ten_lop_hoc","Ngay_khai_giang","Ngay_ket_thuc","So_buoi_tuyen_sinh","So_buoi_hoc","Trang_thai","Ma_khoa_hoc"]]
df['Ngay_khai_giang'] = pd.to_datetime(df['Ngay_khai_giang']).fillna(pd.to_datetime('1999-01-01'))
df['Ngay_ket_thuc'] = pd.to_datetime(df['Ngay_ket_thuc']).fillna(pd.to_datetime('1999-01-01'))
df.to_csv("~/DWH_Cole_Project/data_result/class_transformed.csv", index=False)