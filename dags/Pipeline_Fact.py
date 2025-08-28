from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

local_tz = timezone("Asia/Ho_Chi_Minh")

# Hàm tiện ích: log lỗi kèm task_id và timestamp
def bash_with_error_log(python_file: str) -> str:
    return (
        f'bash -c \'python {python_file} 2>&1 | while read line; '
        f'do echo "[`date "+%Y-%m-%d %H:%M:%S"`][{python_file}] $line" >> ~/DWH_Cole_Project/log_error.txt; done\''
    )

with DAG(
    dag_id="Fact_etl_pipeline",
    start_date=datetime(2025, 8, 15, tzinfo=local_tz),
    schedule="0 4 * * *",
    catchup=False
) as dag:

    # Fact Doanh thu TOT
    extract_Doanh_thu_TOT = BashOperator(task_id="extract_fact_Doanh_thu_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Doanh_thu_TOT.py"))
    transform_Doanh_thu_TOT = BashOperator(task_id="transform_fact_Doanh_thu_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Doanh_thu_TOT.py"))
    load_Doanh_thu_TOT = BashOperator(task_id="load_fact_Doanh_thu_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Doanh_thu_TOT.py"))
    
    # Fact Doanh thu TOA
    extract_Doanh_thu_TOA = BashOperator(task_id="extract_fact_Doanh_thu_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Doanh_thu_TOA.py"))
    transform_Doanh_thu_TOA = BashOperator(task_id="transform_fact_Doanh_thu_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Doanh_thu_TOA.py"))
    load_Doanh_thu_TOA = BashOperator(task_id="load_fact_Doanh_thu_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Doanh_thu_TOA.py"))

    # Fact Doanh số TOT
    extract_Doanh_so_TOT = BashOperator(task_id="extract_fact_Doanh_so_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Doanh_so_TOT.py"))
    transform_Doanh_so_TOT = BashOperator(task_id="transform_fact_Doanh_so_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Doanh_so_TOT.py"))
    load_Doanh_so_TOT = BashOperator(task_id="load_fact_Doanh_so_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Doanh_so_TOT.py"))

    # Fact Doanh số TOA
    extract_Doanh_so_TOA = BashOperator(task_id="extract_fact_Doanh_so_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Doanh_so_TOA.py"))
    transform_Doanh_so_TOA = BashOperator(task_id="transform_fact_Doanh_so_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Doanh_so_TOA.py"))
    load_Doanh_so_TOA = BashOperator(task_id="load_fact_Doanh_so_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Doanh_so_TOA.py"))

    # Fact L7 + L8 TOA
    extract_L7_8_TOA = BashOperator(task_id="extract_fact_L7_8_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/L7_8_TOA.py"))
    transform_L7_8_TOA = BashOperator(task_id="transform_fact_L7_8_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/L7_8_TOA.py"))
    load_L7_8_TOA = BashOperator(task_id="load_fact_L7_8_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/L7_8_TOA.py"))

    # Fact L1 -> L8 Chuyển đổi
    extract_L1_8_FA= BashOperator(task_id="extract_L1_8_FA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/L1_8_FA.py"))
    
    # Fact L1 -> L8 Messenger
    extract_L1_8_Mess=BashOperator(task_id="extract_L1_8_Mess", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/L1_8_Mess.py"))
    transform_L1_8_Mess=BashOperator(task_id="transform_L1_8_Mess", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/L1_8_Mess.py"))
    
    # Fact Chi phí chạy Chuyển đổi TOT
    extract_Chi_phi_FA_TOT=BashOperator(task_id="extract_Chi_phi_FA_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Chi_phi_FA_TOT.py"))
    transform_Chi_phi_FA_TOT=BashOperator(task_id="transform_Chi_phi_FA_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_FA_TOT.py"))
    load_Chi_phi_FA_TOT=BashOperator(task_id="load_Chi_phi_FA_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_FA_TOT.py"))
    load_L7_8_FA_TOT=BashOperator(task_id="load_L7_8_FA_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/L7_8_FA_TOT.py" ))
    
    # Fact Chi phí chạy Chuyển đổi TOA
    transform_Chi_phi_FA_TOA=BashOperator(task_id="transform_Chi_phi_FA_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_FA_TOA.py"))
    load_Chi_phi_FA_TOA=BashOperator(task_id="load_Chi_phi_FA_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_FA_TOA.py"))
    load_L7_8_FA_TOA=BashOperator(task_id="load_L7_8_FA_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/L7_8_FA_TOA.py"))

    # Fact Chi phí chạy Messenger TOT
    extract_Chi_phi_Mess_TOT=BashOperator(task_id="extract_Chi_phi_Mess_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Chi_phi_Mess_TOT.py"))
    transform_Chi_phi_Mess_TOT=BashOperator(task_id="transform_Chi_phi_Mess_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_Mess_TOT.py"))
    load_Chi_phi_Mess_TOT=BashOperator(task_id="load_Chi_phi_Mess_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_Mess_TOT.py"))
    load_L7_8_Mess_TOT=BashOperator(task_id="load_L7_8_Mess_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/L7_8_Mess_TOT.py"))

    # Fact Chi phí chạy Messenger TOA
    transform_Chi_phi_Mess_TOA=BashOperator(task_id="transform_Chi_phi_Mess_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_Mess_TOA.py"))
    load_Chi_phi_Mess_TOA=BashOperator(task_id="load_Chi_phi_Mess_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_Mess_TOA.py"))
    load_L7_8_Mess_TOA=BashOperator(task_id="load_L7_8_Mess_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/L7_8_Mess_TOA.py"))

    # Chiến dịch Meta
    extract_Chien_dich_Meta=BashOperator(task_id="extract_Chien_dich_Meta", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Chien_dich_Meta.py"))
    transform_Chien_dich_Meta=BashOperator(task_id="transform_Chien_dich_Meta", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Chien_dich_Meta.py"))
    load_Chien_dich_Meta=BashOperator(task_id="load_Chien_dich_Meta", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Chien_dich_Meta.py"))
    
    # Fact Chi phí Branding
    extract_Chi_phi_Branding=BashOperator(task_id="extract_Chi_phi_Branding", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Chi_phi_Branding.py"))
    transform_Chi_phi_Branding=BashOperator(task_id="transform_Chi_phi_Branding", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_Branding.py"))
    load_Chi_phi_Branding=BashOperator(task_id="load_Chi_phi_Branding", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_Branding.py"))
    
    # Fact Chi phí CTV
    extract_Chi_phi_CTV=BashOperator(task_id="extract_Chi_phi_CTV", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Chi_phi_CTV.py"))
    transform_Chi_phi_CTV=BashOperator(task_id="transform_Chi_phi_CTV", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Chi_phi_CTV.py"))
    load_Chi_phi_CTV=BashOperator(task_id="load_Chi_phi_CTV", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Chi_phi_CTV.py"))
                                                                           
    # Fact Kế hoạch Marketing
    extract_Ke_hoach_Marketing=BashOperator(task_id="extract_Ke_hoach_Marketing", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Ke_hoach_Marketing.py"))
    transform_Ke_hoach_Marketing=BashOperator(task_id="transform_Ke_hoach_Marketing", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Ke_hoach_Marketing.py"))
    load_Ke_hoach_Marketing_Thang=BashOperator(task_id="load_Ke_hoach_Marketing_Thang", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Ke_hoach_Marketing_Thang.py"))
    load_Ke_hoach_Marketing_Tuan=BashOperator(task_id="load_Ke_hoach_Marketing_Tuan", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Ke_hoach_Marketing_Tuan.py"))

    # Fact Kế hoạch Sale
    extract_Ke_hoach_Sale=BashOperator(task_id="extract_Ke_hoach_Sale", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/extract/Fact/Ke_hoach_Sale.py"))
    transform_Ke_hoach_Sale=BashOperator(task_id="transform_Ke_hoach_Sale", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact/Ke_hoach_Sale.py"))
    load_Ke_hoach_Sale_TOA=BashOperator(task_id="load_Ke_hoach_Sale_TOA", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Ke_hoach_Sale_TOA.py"))
    load_Ke_hoach_Sale_TOT=BashOperator(task_id="load_Ke_hoach_Sale_TOT", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact/Ke_hoach_Sale_TOT.py"))

    # Fact Tổng kết Marketing
    transform_Tong_ket_Marketing=BashOperator(task_id="transform_Tong_ket_Marketing", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/transform/Fact_in_BI/Tong_ket_so_Marketing.py"))
    load_Tong_ket_Marketing=BashOperator(task_id="load_Tong_ket_Marketing", bash_command=bash_with_error_log("~/DWH_Cole_Project/src/load/Fact_in_BI/Tong_ket_so_Marketing.py"))

    # --- Các flow tuyến tính ---
    extract_Doanh_thu_TOT >> transform_Doanh_thu_TOT >> load_Doanh_thu_TOT
    extract_Doanh_thu_TOA >> transform_Doanh_thu_TOA >> load_Doanh_thu_TOA
    extract_Doanh_so_TOT >> transform_Doanh_so_TOT >> load_Doanh_so_TOT
    extract_Doanh_so_TOA >> transform_Doanh_so_TOA >> load_Doanh_so_TOA
    extract_L7_8_TOA >> transform_L7_8_TOA >> load_L7_8_TOA

    # --- Flow FA ---
    extract_L1_8_FA >> extract_Chi_phi_FA_TOT >> transform_Chi_phi_FA_TOT

    transform_Chi_phi_FA_TOT >> transform_Chi_phi_Mess_TOT
    transform_Chi_phi_FA_TOT >> transform_Chi_phi_FA_TOA
    transform_Chi_phi_FA_TOT >> load_Chi_phi_FA_TOT
    transform_Chi_phi_FA_TOT >> load_L7_8_FA_TOT
    
    transform_Chi_phi_FA_TOA >> load_Chi_phi_FA_TOA
    transform_Chi_phi_FA_TOA >> load_L7_8_FA_TOA

    # --- Flow Mess ---
    extract_L1_8_Mess >> transform_L1_8_Mess >> extract_Chi_phi_Mess_TOT >> transform_Chi_phi_Mess_TOT
    transform_Chi_phi_Mess_TOT >> load_Chi_phi_Mess_TOT
    transform_Chi_phi_Mess_TOT >> load_L7_8_Mess_TOT
    transform_Chi_phi_Mess_TOT >> transform_Chi_phi_Mess_TOA
    transform_Chi_phi_Mess_TOA >> load_Chi_phi_Mess_TOA
    transform_Chi_phi_Mess_TOA >> load_L7_8_Mess_TOA

    # --- Flow Branding ---
    extract_Chi_phi_Branding >> transform_Chi_phi_Branding >> load_Chi_phi_Branding

    # --- Flow CTV ---
    extract_Chi_phi_CTV >> transform_Chi_phi_CTV >> load_Chi_phi_CTV

    # --- Lấy dữ liệu Chi phí FA, Mess TOT thì mới chạy Meta ---
    [extract_Chi_phi_FA_TOT, extract_Chi_phi_Mess_TOT, extract_Chi_phi_Branding, extract_Chi_phi_CTV] >> extract_Chien_dich_Meta
    extract_Chien_dich_Meta >> transform_Chien_dich_Meta >> load_Chien_dich_Meta

    # Flow Kế hoạch Marketing
    extract_Ke_hoach_Marketing >> transform_Ke_hoach_Marketing
    transform_Ke_hoach_Marketing >> load_Ke_hoach_Marketing_Thang
    transform_Ke_hoach_Marketing
