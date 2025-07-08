from src.Get_data_DB import DataTransformer
import os
import requests
import pandas as pd
from datetime import date
from dotenv import load_dotenv

# Tạo instance của class
transformer = DataTransformer()

# Load .env
load_dotenv()

ACCESS_TOKEN = os.getenv("Cole_token")
AD_ACCOUNT_ID = os.getenv("Mkt")

#-----------Lần đầu chạy----------
paused_campaign_query = """
    SELECT STT AS campaign_id, 
           Chien_dich AS campaign_name,
           Ngay_bat_dau 
    FROM Chien_dich_Meta
    WHERE Account = 'Mkt'
      AND Trang_thai = 'PAUSED'
"""

df = transformer.fetch_from_sql_server(paused_campaign_query)

# ===== Danh sách chiến dịch đã dừng =====
paused_campaigns = df.to_dict(orient="records")

# Hàm lấy spend theo ngày
def fetch_campaign_metrics(campaign_id, access_token, start_date, end_date):
    base_url = f"https://graph.facebook.com/v20.0/{campaign_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "spend,date_start,messaging_conversations_started",
        "time_range": f'{{"since":"{start_date}", "until":"{end_date}"}}',
        "time_increment": 1,
        "limit": 150
    }

    all_data = []
    url = base_url

    while url:
        res = requests.get(url, params=params if '?' not in url else None)
        res.raise_for_status()
        data = res.json()

        all_data.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")

    return all_data

# ===== Lấy chi phí theo ngày =====
today = date.today().strftime("%Y-%m-%d")
all_rows = []

print(f"🔍 Tổng số chiến dịch PAUSED: {len(paused_campaigns)}")

for campaign in paused_campaigns:
    campaign_id = campaign["campaign_id"]
    campaign_name = campaign["campaign_name"]
    start_date = campaign["Ngay_bat_dau"].strftime("%Y-%m-%d") if pd.notnull(campaign["Ngay_bat_dau"]) else "2024-01-01"

    try:
        print(f"📊 Đang lấy dữ liệu spend của chiến dịch {campaign_name} từ {start_date}")
        spend_data = fetch_campaign_metrics(campaign_id, ACCESS_TOKEN, start_date, today)

        for d in spend_data:
            all_rows.append({
                "Campaign ID": campaign_id,
                "Campaign Name": campaign_name,
                "Date": d["date_start"],
                "Spend": float(d["spend"])
            })
    except Exception as e:
        print(f"⚠️ Lỗi khi lấy dữ liệu chiến dịch {campaign_name}: {e}")

# ===== Ghi dữ liệu ra CSV =====
df_spend = pd.DataFrame(all_rows)
output_path = os.path.expanduser("~/DWH_Cole_Project/data_tmp/spend_mess_PAUSED.csv")
df_spend.to_csv(output_path, index=False)
print(f"✅ Đã ghi dữ liệu vào: {output_path}")
