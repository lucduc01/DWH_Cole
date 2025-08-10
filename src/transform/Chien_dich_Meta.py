import pandas as pd
import os

# Đường dẫn đọc và ghi file
input_dir = os.path.expanduser("~/DWH_Cole_Project/data_tmp")
output_dir = os.path.expanduser("~/DWH_Cole_Project/data_result")
output_file = os.path.join(output_dir, "Chien_dich_meta.csv")

os.makedirs(output_dir, exist_ok=True)

# Tên file tương ứng với tài khoản
account_files = {
    "C9": "campaigns_C9.csv",
    "Mkt": "campaigns_Mkt.csv",
    "Cole8": "campaigns_Cole8.csv",
    "Branding": "campaigns_Branding.csv",
    "CTV": "campaigns_CTV.csv"
}

all_data = []

for account, filename in account_files.items():
    file_path = os.path.join(input_dir, filename)

    # Đọc dữ liệu
    df = pd.read_csv(file_path)

    # Chuẩn hóa dữ liệu theo cấu trúc SQL Server
    df["STT"] = df["id"]
    df["Account"] = account
    df["Chien_dich"] = df["name"]
    df["Ngay_bat_dau"] = pd.to_datetime(df["start_time"].str[:10])
    df["Trang_thai"] = df["effective_status"]

    # Chỉ giữ các cột phù hợp với bảng SQL Server
    df_final = df[["STT", "Account", "Chien_dich", "Ngay_bat_dau", "Trang_thai"]]

    all_data.append(df_final)

# Gộp tất cả dữ liệu
final_df = pd.concat(all_data, ignore_index=True)

# Ghi ra file CSV kết quả
final_df.to_csv(output_file, index=False)

print(f"✅ Đã ghi dữ liệu ra file: {output_file}")

