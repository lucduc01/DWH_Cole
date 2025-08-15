from src.Get_data_DB import DataTransformer
import os
import requests
import pandas as pd
from datetime import date
from dotenv import load_dotenv

# Load biến môi trường từ file .env
load_dotenv()

# Khởi tạo transformer để truy vấn SQL Server
transformer = DataTransformer()

# Truy vấn danh sách chiến dịch đã dừng
paused_campaign_query = """
    SELECT STT AS campaign_id, 
           Chien_dich AS campaign_name,
           Ngay_bat_dau
    FROM Chien_dich_Meta
    WHERE Account  = 'CTV'
     AND Trang_thai ='ACTIVE'
"""
df = transformer.fetch_from_sql_server(paused_campaign_query)

# Hàm lấy chi phí theo ngày từ Facebook Graph API
def fetch_campaign_spend(campaign_id, access_token, start_date, end_date):
    url = f"https://graph.facebook.com/v20.0/{campaign_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "spend,date_start",
        "time_range": f'{{"since":"{start_date}", "until":"{end_date}"}}',
        "time_increment": 1,
        "limit": 100
     
    }

    res = requests.get(url, params=params)
    res.raise_for_status()
    return res.json().get("data", [])

# Lấy ngày hôm nay
today = date.today().strftime("%Y-%m-%d")

# Xử lý cho tài khoản: C9 

access_token = os.getenv("Cole_token")
account_campaigns = df.to_dict(orient="records")
all_rows = []

for campaign in account_campaigns:
    campaign_id = campaign["campaign_id"]
    campaign_name = campaign["campaign_name"]
    start_date = campaign["Ngay_bat_dau"].strftime("%Y-%m-%d") if pd.notnull(campaign["Ngay_bat_dau"]) else "2024-01-01"

    try:
        print(f"📊 Lấy dữ liệu spend: {campaign_name} ({campaign_id}) từ {start_date}")
        spend_data = fetch_campaign_spend(campaign_id, access_token, start_date, today)
        for d in spend_data:
            all_rows.append({
                "Campaign ID": campaign_id,
                "Campaign Name": campaign_name,
                "Date": d["date_start"],
                "Spend": float(d["spend"])
            })
    except Exception as e:
        print(f"⚠️ Lỗi với chiến dịch {campaign_name} ({campaign_id}): {e}")

    # Ghi dữ liệu vào file CSV theo từng tài khoản
df_spend = pd.DataFrame(all_rows)
output_path = os.path.expanduser(f"~/DWH_Cole_Project/data_tmp/spend_CTV.csv")
df_spend.to_csv(output_path, index=False)
print(f"✅ Đã ghi file CSV cho : {output_path}")

