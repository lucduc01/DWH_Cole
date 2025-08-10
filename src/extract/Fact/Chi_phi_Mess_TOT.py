import os
import requests
import pandas as pd
from datetime import date
from dotenv import load_dotenv
from src.Get_data_DB import DataTransformer
import time

# Khởi tạo
transformer = DataTransformer()
load_dotenv()

ACCESS_TOKEN = os.getenv("Cole_token")
today = date.today().strftime("%Y-%m-%d")

# Truy vấn danh sách Campaign
query = """
    SELECT STT AS campaign_id,
           Chien_dich AS campaign_name,
           Ngay_bat_dau
    FROM Chien_dich_Meta
    WHERE Account = 'Mkt'
      AND  Trang_thai = 'ACTIVE'
"""
df_campaigns = transformer.fetch_from_sql_server(query)
campaigns = df_campaigns.to_dict(orient="records")

# === Hàm gọi API an toàn ===
def safe_request(url, params=None, max_retries=5, delay=600):
    for attempt in range(max_retries):
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            try:
                err = response.json().get("error", {})
                code = err.get("code")
                if code == 17:
                    print(f"⏳ Quá nhiều yêu cầu. Đợi {delay}s... (Thử lại {attempt+1}/{max_retries})")
                    time.sleep(delay)
                    continue
            except Exception:
                pass
            print("⛔ Lỗi:", response.text)
            raise Exception(response.text)
    raise Exception("🚫 Quá số lần thử lại.")

# === Lấy danh sách Adset có optimization_goal = CONVERSATIONS ===
def get_adsets_with_conversations(campaign_id, access_token):
    url = f"https://graph.facebook.com/v20.0/{campaign_id}/adsets"
    params = {
        "access_token": access_token,
        "fields": "id,name,start_time,status,optimization_goal",
        "limit": 100
    }

    adsets = []
    while url:
        data = safe_request(url, params)
        for a in data.get("data", []):
            if a.get("optimization_goal") == "CONVERSATIONS":
                adsets.append(a)
        url = data.get("paging", {}).get("next")
        params = None
    return adsets

# === Lấy insights theo ngày ===
def get_adset_insights(adset_id, access_token, start_date, end_date):
    url = f"https://graph.facebook.com/v20.0/{adset_id}/insights"
    params = {
        "access_token": access_token,
        "fields": "spend,date_start,actions",
        "time_range": f'{{"since":"{start_date}", "until":"{end_date}"}}',
        "time_increment": 1,
        "limit": 100
    }

    data = safe_request(url, params)
    return data.get("data", [])

# === Xử lý toàn bộ dữ liệu ===
results = []

for camp in campaigns:
    campaign_id = camp["campaign_id"]
    campaign_name = camp["campaign_name"]
    start_date = camp["Ngay_bat_dau"].strftime("%Y-%m-%d") if pd.notnull(camp["Ngay_bat_dau"]) else "2025-01-01"

    print(f"📌 Campaign: {campaign_name} ({campaign_id})")
    adsets = get_adsets_with_conversations(campaign_id, ACCESS_TOKEN)

    for adset in adsets:
        adset_id = adset["id"]
        adset_name = adset["name"]
        adset_start = adset.get("start_time", "")
        adset_status = adset.get("status", "")

        print(f"   📊 Adset: {adset_name} ({adset_id})")
        insights = get_adset_insights(adset_id, ACCESS_TOKEN, start_date, today)

        for row in insights:
            started_conversations = 0
            for action in row.get("actions", []):
                if action.get("action_type") == "onsite_conversion.messaging_conversation_started_7d":
                    started_conversations = int(float(action.get("value", 0)))
                    break

            results.append({
                "Campaign ID": campaign_id,
                "Campaign Name": campaign_name,
                "Adset ID": adset_id,
                "Adset Name": adset_name,
                "Adset Start": adset_start,
                "Adset Status": adset_status,
                "Date": row.get("date_start"),
                "Spend": float(row.get("spend", 0)),
                "Conversations Started (7d)": started_conversations
            })

# === Ghi ra file CSV ===
df_result = pd.DataFrame(results)
output_path = "~/DWH_Cole_Project/data_tmp/Chi_phi&So_mess_TOT.csv"
df_result.to_csv(output_path, index=False)
print(f"✅ Đã ghi dữ liệu vào: {output_path}")


