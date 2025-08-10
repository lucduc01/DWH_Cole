import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def read_public_google_sheet(sheet_url):
    """
    Đọc Google Sheet công khai: dòng thứ 2 là header, dòng 3 trở đi là dữ liệu.
    """
    try:
        # Trích xuất ID và gid
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]
        gid = sheet_url.split('#gid=')[1] if '#gid=' in sheet_url else '0'

        # URL dạng CSV
        csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'

        # Đọc toàn bộ không có header
        raw_df = pd.read_csv(csv_url, header=None)

        # Gán dòng thứ 2 (index=1) làm tên cột
        df = raw_df.iloc[2:].copy()
        df.columns = raw_df.iloc[1]

        print("✅ Đọc dữ liệu thành công (dòng 2 làm header).")
        return df

    except Exception as e:
        print(f"❌ Lỗi khi đọc Google Sheet: {e}")
        return None


# Link CSV tương ứng
Sheet_link=os.getenv("Sheet_Plan_Marketing")

# Đọc dữ liệu
df = read_public_google_sheet(Sheet_link)

# Ghi dữ liệu
df.to_csv("~/DWH_Cole_Project/data_tmp/Marketing_plan.csv", index=False)
