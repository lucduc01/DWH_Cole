import gspread
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import string

load_dotenv()
# --- Đọc dữ liệu từ Google Sheet ---
google_sheet_url = os.getenv('Sheet_Cole_financial')

# --- Cấu hình thư mục lưu trữ token xác thực ---
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Thư mục làm việc: {script_dir}")

# Tạo thư mục credentials nếu chưa tồn tại
credentials_dir = os.path.join(script_dir, '.gspread_credentials')
if not os.path.exists(credentials_dir):
    os.makedirs(credentials_dir)
    print(f"Đã tạo thư mục credentials: {credentials_dir}")

# Đường dẫn đến các file cần thiết
client_secret_path = os.path.join(script_dir, 'client_secret.json')
token_path = os.path.join(credentials_dir, 'token.json')

# --- Xác thực với Google Sheets ---
try:


    # Dùng OAuth thủ công
    if os.path.exists(client_secret_path):
        # Kiểm tra nếu đã có token lưu trữ
        if os.path.exists(token_path):
            gc = gspread.oauth(
                credentials_filename=client_secret_path,
                authorized_user_filename=token_path
            )
            print("✅ Xác thực thành công với token đã lưu")
        else:
            # Tạo flow xác thực
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )

            credentials = flow.run_local_server(port=0, open_browser=False)
            
            # Lưu token để dùng lại
            with open(token_path, 'w') as token_file:
                token_file.write(credentials.to_json())
            
            print(f"✅ Đã lưu token vào {token_path}")
            
            # Tạo client gspread
            gc = gspread.authorize(credentials)
            print("✅ Xác thực thành công với Google Sheets")
    
    else:
        raise FileNotFoundError("Không tìm thấy file client_secret.json hoặc service_account.json")

except Exception as e:
    print(f"\n❌ LỖI XÁC THỰC: {e}")
    print("\nHƯỚNG DẪN KHẮC PHỤC:")
    print("1. Đảm bảo có ít nhất một file credentials trong thư mục:")
    print(f"   - {script_dir}")
    print("\nĐối với client_secret.json:")
    print("   - Tạo OAuth 2.0 Client ID từ Google Cloud Console")
    print("   - Tải file JSON và đặt tên là client_secret.json")
    print("\nĐối với service_account.json:")
    print("   - Tạo Service Account trong Google Cloud Console")
    print("   - Cấp quyền Editor cho Google Sheet")
    print("   - Tải file JSON và đặt tên là service_account.json")
    exit()

try:
    print(f"\nĐang kết nối với Google Sheet: {google_sheet_url}")
    spreadsheet = gc.open_by_url(google_sheet_url)
    worksheet_name = "S2-Sổ ghi nhận chi phí"
    worksheet = spreadsheet.worksheet(worksheet_name)

    # Lấy dữ liệu
    print(f"Đang đọc dữ liệu từ worksheet '{worksheet_name}'...")
    # Hàm chuyển chữ cột sang chỉ số
    def col_letter_to_index(letter):
        return string.ascii_uppercase.index(letter.upper())

    # Các cột muốn lấy
    target_columns = ['B', 'F', 'G', 'M']
    target_indexes = [col_letter_to_index(col) for col in target_columns]

    # Lấy toàn bộ dữ liệu từ sheet
    rows = worksheet.get_all_values()

    # Lấy từ dòng thứ 4 (header) và các dòng dữ liệu tiếp theo
    if len(rows) >= 4:
        header_row = rows[3]
        data_rows = rows[4:]

        # Lọc theo các cột mong muốn
        selected_headers = [header_row[i] for i in target_indexes]
        filtered_data = [
            [row[i] if i < len(row) else '' for i in target_indexes]
            for row in data_rows
        ]

        df = pd.DataFrame(filtered_data, columns=selected_headers)

        # Lưu ra file nếu muốn
        df.to_csv("~/DWH_Cole_Project/data_tmp/Chi_phi_Khác.csv", index=False)
        print(f"\n✅ Đã lưu dữ liệu vào ")
    else:
        print("❌ Sheet không đủ dữ liệu để lấy từ dòng thứ 4")

except gspread.exceptions.SpreadsheetNotFound:
    print(f"\n❌ Không tìm thấy Google Sheet tại URL đã cung cấp")
    print("Vui lòng kiểm tra lại URL và quyền truy cập")
except gspread.exceptions.WorksheetNotFound:
    print(f"\n❌ Không tìm thấy worksheet '{worksheet_name}'")
    print(f"Các worksheet có sẵn: {[ws.title for ws in spreadsheet.worksheets()]}")
except Exception as e:
    print(f"\n❌ LỖI KHÔNG XÁC ĐỊNH: {type(e).__name__}: {e}")