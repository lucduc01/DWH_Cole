import pandas as pd
from src.Get_data_DB import DataTransformer
# Tạo instance của class
transformer = DataTransformer()

Query_Doanh_thu='select Doanh_thu, Ngay_chuyen_tien as Ngay, Ma_khoa_hoc,Ma_saler,Ma_marketer,Ma_kenh from Fact_Doanh_thu_TOT'
Query_Doanh_so='select Doanh_so, Ngay_chuyen_tien as Ngay, Ma_khoa_hoc,Ma_saler,Ma_marketer,Ma_kenh from Fact_Doanh_so_TOT'
Query_Level= 'select L1,L1_L1C, L2,L3,L6,L7,L8,Ngay,Ma_khoa_hoc,Ma_saler,Ma_marketer,Ma_kenh  from Fact_Count_Level_TOT'

df_doanh_thu=transformer.fetch_from_sql_server(Query_Doanh_thu)
df_doanh_so= transformer.fetch_from_sql_server(Query_Doanh_so)
df_level=transformer.fetch_from_sql_server(Query_Level)

""" Tính tổng lại doanh thu và doanh số vì ở Fact ban đầu có cột Id nên tạo thành 2 bản ghi riêng biệt mặc dù 
các chiều dữ liệu đều trùng lặp. Cần sum trước khi join để tránh sinh thừa dữ liệu"""
df_doanh_thu = df_doanh_thu.groupby(
    ["Ngay", "Ma_khoa_hoc", "Ma_saler", "Ma_marketer", "Ma_kenh"],
    as_index=False
)["Doanh_thu"].sum()

df_doanh_so = df_doanh_so.groupby(
    ["Ngay", "Ma_khoa_hoc", "Ma_saler", "Ma_marketer", "Ma_kenh"],
    as_index=False
)["Doanh_so"].sum()

# Đồng nhất lại toàn bộ kiểu dữ liệu ngày 
df_doanh_thu["Ngay"] = pd.to_datetime(df_doanh_thu["Ngay"]).dt.date
df_doanh_so["Ngay"] = pd.to_datetime(df_doanh_so["Ngay"]).dt.date
df_level["Ngay"] = pd.to_datetime(df_level["Ngay"]).dt.date

# Join toàn bộ dữ liệu của 3 dataframe
merged1=df_doanh_thu.merge(df_doanh_so,on=['Ngay','Ma_khoa_hoc','Ma_saler','Ma_marketer','Ma_kenh'],how='outer')
merged1 = merged1.fillna(0)
merged2=merged1.merge(df_level, on=['Ngay','Ma_khoa_hoc','Ma_saler','Ma_marketer','Ma_kenh'],how='outer')
merged2 = merged2.fillna(0)

# Tạo cột nhóm marketer phân nhóm cho Mã marketing
def gán_nhom_marketer(ma):
    if ma in ["BinhND", "HienPT"]:
        return "Ads"
    elif ma in ["HangNT", "ToUyen", "DangHang", "HuyenTrang"]:
        return "Product MKT"
    else:
        return "Khác"

merged2["Nhom_marketer"] = merged2["Ma_marketer"].apply(gán_nhom_marketer)

# Ghi dữ liệu cuối cùng 
merged2.to_csv("~/DWH_Cole_Project/data_result/Tong_ket_Marketing.csv", index=False)