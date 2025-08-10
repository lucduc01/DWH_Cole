import pandas as pd
df=pd.read_csv("~/DWH_Cole_Project/data_tmp/spend_Branding.csv")
df = df.drop(columns=['Campaign ID', 'Campaign Name'])

# Tính tổng cột Spend theo cột Date
df = df.groupby('Date', as_index=False)['Spend'].sum()

# Đổi tên các cột
df = df.rename(columns={'Date': 'Thoi_gian', 'Spend': 'Chi_phi'})
df.to_csv("~/DWH_Cole_Project/data_result/Chi_phi_Branding_transformed.csv", index=False)