import pandas as pd
from src.Get_data_DB import DataTransformer
transformer = DataTransformer()

Offline_class_query = """ select Ma_lop_hoc,
                                 Ngay_khai_giang,
                                 So_buoi_tuyen_sinh
                          from Dim_Lop_hoc
"""
# Có 2 cách tiếp cận phân nhánh lớp thuộc khoá học. 
# Cách 1: Lớp thuộc nhiều sản phẩm ( sản phẩm lẻ và sản phẩm combo). 
# Cách 2: Lớp thuộc về 1 sản phẩm suy nhất . Trong Dim đang tổ chức theo cách 2 nhưng từ Tên khoá học của Chiến dịch Meta muốn 
# về lớp thì phải dùng Cách 1
Mapping_class_query="""select oc.id Ma_lop_hoc,
                               p.id Ma_khoa_hoc
                        from offline_classes oc
                        join product_items pi on pi.item_id =oc.item_id
                        join products p on pi.product_id =p.id"""
df_LH = transformer.fetch_from_sql_server(Offline_class_query)
df_mapping=transformer.fetch_from_mysql(Mapping_class_query)
df= pd.read_csv("~/DWH_Cole_Project/data_result/Chi_phi_FA_transformed.csv")
df_78=pd.read_csv("~/DWH_Cole_Project/data_result/L78_FA_transformed.csv")

# Gộp lại dữ liệu L7, L8 vào dữ liệu gốc
df=df.merge(df_78, on=['Thoi_gian','Ma_khoa_hoc','Ma_marketer'],how='outer')

# Join để bảng ánh xa Lớp thuộc những sản phẩm nào (1 Lớp sẽ thuộc nhiều sản phẩm, đặc biệt sản phẩm bán có COMBO)
df_LH=df_LH.merge(df_mapping, on='Ma_lop_hoc', how='inner')
df['Thoi_gian'] = pd.to_datetime(df['Thoi_gian'])
df_LH['Ngay_khai_giang'] = pd.to_datetime(df_LH['Ngay_khai_giang'])

# Join dữ liệu để Chi phí về lớp
merged = df.merge(df_LH,on='Ma_khoa_hoc', how='inner')

# Điều kiện 1: So_buoi_tuyen_sinh là 0 hoặc 1 và Thoi_gian <= Ngay_khai_giang
# Đây là trường hợp sản phẩm đó mới chỉ mở 1 lớp hoặc nó thuộc lớp đầu tiên của sản phẩm đó
cond1 = (merged["So_buoi_tuyen_sinh"].isin([0, 1])) & (merged["Thoi_gian"] <= merged["Ngay_khai_giang"])

# Điều kiện 2: So_buoi_tuyen_sinh khác -1 và Thoi_gian nằm trong khoảng (Ngay_khai_giang - So_buoi_tuyen_sinh, Ngay_khai_giang)
# Dữ liệu CHi phí được lấy từ 2014, từ lúc này Ngày khai giảng của các lớp đều được điền đầy đủ
cond2 = (~merged["So_buoi_tuyen_sinh"].isin([-1])) & (
    merged["Thoi_gian"] > (merged["Ngay_khai_giang"] - pd.to_timedelta(merged["So_buoi_tuyen_sinh"], unit="D"))
) & (merged["Thoi_gian"] <= merged["Ngay_khai_giang"])

# Lọc các bản ghi thỏa mãn điều kiện
valid_records = merged[cond1 | cond2]

# Xử lí riêng cho trường hợp Chi phí chạy cho sản phẩm COMBO, khi này 1 chi phí bị lặp lại cho nhiều lớp thuộc sản phẩm đó
# Bước 1: Nhóm dữ liệu theo Thoi_gian và Ma_khoa_hoc
grouped = valid_records.groupby(['Thoi_gian', 'Ma_khoa_hoc'])

# Bước 2: Tạo hàm xử lý cho mỗi nhóm
def adjust_group_values(group):
    count = len(group)
    if count > 1:
        # Lấy giá trị đầu tiên của mỗi cột
        first_values = {
            'Chi_phi': group['Chi_phi'].iloc[0],
            'L1': group['L1'].iloc[0],
            'L1_L1C': group['L1_L1C'].iloc[0],
            'L7': group['L7'].iloc[0],
            'L8': group['L8'].iloc[0]
        }
        
        # Tính giá trị mới cho mỗi cột và làm tròn
        adjusted_values = {
            'Chi_phi': round(first_values['Chi_phi'] / count),
            'L1': round(first_values['L1'] / count),
            'L1_L1C': round(first_values['L1_L1C'] / count),
            'L7': round(first_values['L7'] / count),
            'L8': round(first_values['L8'] / count)
        }
        
        # Gán giá trị mới cho tất cả các bản ghi trong nhóm
        group['Chi_phi'] = adjusted_values['Chi_phi']
        group['L1'] = adjusted_values['L1']
        group['L1_L1C'] = adjusted_values['L1_L1C']
        group['L7'] = adjusted_values['L7']
        group['L8'] = adjusted_values['L8']
    
    return group

# Bước 3: Áp dụng hàm xử lý cho từng nhóm
valid_records_processed = grouped.apply(adjust_group_values)
# Bước 4: Reset index nếu cần (do groupby tạo multi-index)
valid_records_processed = valid_records_processed.reset_index(drop=True)

# Lấy các cột cần thiết
final_L78=valid_records_processed[['Thoi_gian','Ma_lop_hoc','Ma_marketer','L7','L8']]
final=valid_records_processed[['Chi_phi','Thoi_gian','Ma_lop_hoc','Ma_marketer','L1','L1_L1C']]

final.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_FA_TOA_transformed.csv",index=False)
final_L78.to_csv("~/DWH_Cole_Project/data_result/L78_FA_TOA_transformed.csv",index=False)
