import pandas as pd

# Đọc dữ liệu chi phí 
df=pd.read_csv("~/DWH_Cole_Project/data_tmp/Chi_phi_Khác.csv")

df['Ngày giao dịch (TOT)'] = pd.to_datetime(df['Ngày giao dịch (TOT)'], format='%d/%m/%Y', errors='coerce')

# Lọc những dòng có ngày từ 2024-01-01 trở đi
df = df[df['Ngày giao dịch (TOT)'] >= pd.to_datetime('2024-01-01')].copy()
df = df.dropna(subset=['Ngày giao dịch (TOT)'])

# Nếu muốn hiển thị lại ngày ở định dạng YYYY-MM-DD (chuỗi)
df['Ngày giao dịch (TOT)'] = df['Ngày giao dịch (TOT)'].dt.strftime('%Y-%m-%d')

df.rename(columns={' Chi nhân sự ': 'Chi nhân sự'}, inplace=True)
df['Số tiền chi ra'] = df['Số tiền chi ra'].str.replace(r'[^0-9]', '', regex=True).astype(int)
df['Tên nhóm chi'] = df.apply(
    lambda row: f"{row['Loại chi']} Nhân sự" if row['Chi nhân sự'] == 'Có' else f"{row['Loại chi']} Phi nhân sự",
    axis=1
)

# Pivot bảng
df_pivot = df.pivot_table(
    index='Ngày giao dịch (TOT)',
    columns='Tên nhóm chi',
    values='Số tiền chi ra',
    aggfunc='sum',  # dùng sum nếu có nhiều dòng cùng ngày + loại chi
    fill_value=0
).reset_index()
df=df_pivot.drop(columns=['Marketing Phi nhân sự', 'Marketing Nhân sự'], inplace=True)

# Ghi dữ liệu
df.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_Khác_transformed.csv", index=False)


