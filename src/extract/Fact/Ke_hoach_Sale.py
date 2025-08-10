import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def read_public_google_sheet_1(sheet_url):
    """
    Đọc Sheet 1: dòng thứ 4 là header, dòng 5 trở đi là dữ liệu.
    Giữ nguyên gid trong URL.
    """
    try:
        # Trích xuất ID và gid
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]
        gid = sheet_url.split('#gid=')[1] if '#gid=' in sheet_url else '0'

        # URL dạng CSV
        csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'

        # Đọc toàn bộ không có header
        raw_df = pd.read_csv(csv_url, header=None)

        # Gán dòng thứ 4 (index=3) làm tên cột
        df = raw_df.iloc[4:].copy()
        df.columns = raw_df.iloc[3]

        print("✅ Đọc Sheet 1 thành công (dòng 4 làm header).")
        return df

    except Exception as e:
        print(f"❌ Lỗi khi đọc Sheet 1: {e}")
        return None


def read_public_google_sheet_2(sheet_url):
    """
    Đọc Sheet 2: dòng thứ 2 là header, dòng 3 trở đi là dữ liệu.
    Thay thế gid bằng 1489811475.
    """
    try:
        # Trích xuất ID
        sheet_id = sheet_url.split('/d/')[1].split('/')[0]
        
        # Thay thế gid bằng 1489811475
        gid = '1489811475'

        # URL dạng CSV
        csv_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'

        # Đọc toàn bộ không có header
        raw_df = pd.read_csv(csv_url, header=None)

        # Gán dòng thứ 2 (index=1) làm tên cột
        df = raw_df.iloc[2:].copy()
        df.columns = raw_df.iloc[1]

        print("✅ Đọc Sheet 2 thành công (dòng 2 làm header).")
        return df

    except Exception as e:
        print(f"❌ Lỗi khi đọc Sheet 2: {e}")
        return None


def main():
    # Link từ .env
    sheet_link = os.getenv("Sheet_Plan_Sale")
    
    if not sheet_link:
        print("❌ Không tìm thấy Sheet_Plan_Sale trong file .env")
        return
    
    print(f"📊 Đang xử lý Google Sheet: {sheet_link}")
    
    # Đọc Sheet 1
    print("\n--- Đọc Sheet 1 ---")
    df_sheet1 = read_public_google_sheet_1(sheet_link)
    
    if df_sheet1 is not None:
        print(f"Sheet 1: {df_sheet1.shape[0]} dòng, {df_sheet1.shape[1]} cột")
        # Ghi Sheet 1
        output_path_1 = "~/DWH_Cole_Project/data_tmp/Ke_hoach_Sale_TOA.csv"
        df_sheet1.to_csv(output_path_1, index=False)
        print(f"📁 Đã lưu Sheet 1 vào: {output_path_1}")
    
    # Đọc Sheet 2
    print("\n--- Đọc Sheet 2 ---")
    df_sheet2 = read_public_google_sheet_2(sheet_link)
    
    if df_sheet2 is not None:
        print(f"Sheet 2: {df_sheet2.shape[0]} dòng, {df_sheet2.shape[1]} cột")
        # Ghi Sheet 2
        output_path_2 = "~/DWH_Cole_Project/data_tmp/Ke_hoach_Sale_TOT.csv"
        df_sheet2.to_csv(output_path_2, index=False)
        print(f"📁 Đã lưu Sheet 2 vào: {output_path_2}")
    
    return df_sheet1, df_sheet2


if __name__ == "__main__":
    # Chạy chương trình chính
    sheet1_data, sheet2_data = main()
