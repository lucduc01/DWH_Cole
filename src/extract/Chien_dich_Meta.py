import requests
from dotenv import load_dotenv
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta   
import csv

# Load biến môi trường
load_dotenv()

# Đường dẫn ghi file
OUTPUT_DIR = os.path.expanduser("~/DWH_Cole_Project/data_tmp/")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Cấu hình các tài khoản quảng cáo
ACCOUNTS = [
    {
        "name": "Mkt",
        "ad_account_id": os.getenv("Mkt"),
        "access_token": os.getenv("Cole_token")
    },
    {
        "name": "C9",
        "ad_account_id": os.getenv("C9"),
        "access_token": os.getenv("Cole_token")
    },
    {
        "name": "Cole8",
        "ad_account_id": os.getenv("Cole8"),
        "access_token": os.getenv("BM_token")
    },
    {
        "name": "Branding",
        "ad_account_id": os.getenv("Branding"),
        "access_token": os.getenv("Cole_token")
    },
    {
        "name": "CTV",
        "ad_account_id": os.getenv("CTV"),
        "access_token": os.getenv("Cole_token")
    }
]

# Hàm lấy chiến dịch
def get_campaigns(ad_account_id, access_token):
    url = f"https://graph.facebook.com/v20.0/{ad_account_id}/campaigns"
    params = {
        "access_token": access_token,
        "fields": "id,name,start_time,status,effective_status",
        "limit": 100
    }

    campaigns = []

    # 👉 Lấy ngày hiện tại lùi 2 năm 
    min_start_date = datetime.today() - relativedelta(months=24)
   
    while url:
        response = requests.get(url, params=params if '?' not in url else None)
        response.raise_for_status()
        data = response.json()

        for c in data.get("data", []):
            start_time_str = c.get("start_time")
            if start_time_str:
                try:
                    start_dt = datetime.strptime(start_time_str[:10], "%Y-%m-%d")
                    if start_dt >= min_start_date:
                        campaigns.append({
                            "id": c["id"],
                            "name": c.get("name", "No name"),
                            "start_time": start_time_str,
                            "status": c.get("status"),
                            "effective_status": c.get("effective_status")
                        })
                except ValueError:
                    pass  # Bỏ qua lỗi ngày

        url = data.get("paging", {}).get("next")

    return campaigns


# Lặp qua tài khoản và ghi CSV
for account in ACCOUNTS:
    try:
        print(f"📥 Đang lấy dữ liệu chiến dịch cho tài khoản: {account['name']}")
        campaigns = get_campaigns(account["ad_account_id"], account["access_token"])
        
        output_file = os.path.join(OUTPUT_DIR, f"campaigns_{account['name']}.csv")
        with open(output_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "start_time", "status", "effective_status"])
            writer.writeheader()
            writer.writerows(campaigns)

        print(f"✅ Đã ghi {len(campaigns)} chiến dịch vào: {output_file}\n")
    except Exception as e:
        print(f"❌ Lỗi khi xử lý tài khoản {account['name']}: {e}")
