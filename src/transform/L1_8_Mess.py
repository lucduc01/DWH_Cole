import pandas as pd

df1=pd.read_csv("~/DWH_Cole_Project/data_tmp/So_L1C_Mess_TOT.csv")
df2=pd.read_csv("~/DWH_Cole_Project/data_tmp/So_L1_Mess_TOT.csv")
df3=pd.read_csv("~/DWH_Cole_Project/data_tmp/So_L7_Mess_TOT.csv")
df4=pd.read_csv("~/DWH_Cole_Project/data_tmp/So_L8_Mess_TOT.csv")

df_bonus=pd.read_csv("~/DWH_Cole_Project/data_result/Chi_phi_mess_bonus_transformed.csv")


# Join dữ liệu 

merged=df1.merge(df2,on=['Thoi_gian','Ma_khoa_hoc'], how="outer")
merged=merged.merge(df3,on=['Thoi_gian','Ma_khoa_hoc'], how="outer")
merged=merged.merge(df4,on=['Thoi_gian','Ma_khoa_hoc'], how="outer")
# 1. Thay thế giá trị trống (NaN) trong cột 'L1C' bằng 0
# inplace=True sẽ thay đổi trực tiếp trên DataFrame 'merged' mà không cần gán lại
merged = merged.fillna(0)

# 2. Tạo cột mới 'L1_L1C' bằng L1 - L1C
# Đảm bảo rằng cả L1 và L1C đều là kiểu số trước khi thực hiện phép trừ
# Nếu chúng có thể là kiểu object (chuỗi), bạn cần chuyển đổi chúng sang số trước.
merged['L1'] = pd.to_numeric(merged['L1'], errors='coerce')
merged['L1C'] = pd.to_numeric(merged['L1C'], errors='coerce')

merged['L1_L1C'] = merged['L1'] - merged['L1C']

merged=merged[['Thoi_gian','Ma_khoa_hoc','L1','L1_L1C','L7','L8']]

#----Cộng thêm dữ liệu L1 bonus 
df_bonus=df_bonus[['Thoi_gian','Ma_khoa_hoc','L1','L1_L1C','L7','L8']]
df_combined = pd.concat([df_bonus, merged], ignore_index=True)
df_result = df_combined.groupby(['Thoi_gian', 'Ma_khoa_hoc'])[['L1', 'L1_L1C','L7','L8']].sum().reset_index()
df_result = df_result[df_result['Thoi_gian'] >= '2025-01-01']
df_result.to_csv("~/DWH_Cole_Project/data_result/Count_L_of_Mess.csv",index=False)