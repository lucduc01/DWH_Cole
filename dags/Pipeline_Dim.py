from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

local_tz = timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="Dim_etl_pipeline",
    start_date=datetime(2025, 8, 15, tzinfo=local_tz),
    schedule="0 3 * * *",
    catchup=False
) as dag:

    # Dim Saler
    extract_Saler = BashOperator(task_id="extract_dim_Saler", bash_command="python ~/DWH_Cole_Project/src/extract/Dim/Saler.py")
    transform_Saler = BashOperator(task_id="transform_dim_Saler", bash_command="python ~/DWH_Cole_Project/src/transform/Dim/Saler.py")
    load_Saler = BashOperator(task_id="load_dim_Saler", bash_command="python ~/DWH_Cole_Project/src/load/Dim/Saler.py")

    # Dim Marketing
    extract_Marketing = BashOperator(task_id="extract_dim_Marketing", bash_command="python ~/DWH_Cole_Project/src/extract/Dim/Marketing.py")
    transform_Marketing = BashOperator(task_id="transform_dim_Marketing", bash_command="python ~/DWH_Cole_Project/src/transform/Dim/Marketing.py")
    load_Marketing = BashOperator(task_id="load_dim_Marketing", bash_command="python ~/DWH_Cole_Project/src/load/Dim/Marketing.py")

    # Dim Phan_Khoa_hoc
    extract_Phan_Khoa_hoc = BashOperator(task_id="extract_dim_Phan_Khoa_hoc", bash_command="python ~/DWH_Cole_Project/src/extract/Dim/Phan_Khoa_hoc.py")
    transform_Phan_Khoa_hoc = BashOperator(task_id="transform_dim_Phan_Khoa_hoc", bash_command="python ~/DWH_Cole_Project/src/transform/Dim/Phan_Khoa_hoc.py")
    load_Phan_loai = BashOperator(task_id="load_dim_Phan_loai", bash_command="python ~/DWH_Cole_Project/src/load/Dim/Phan_loai.py")
    load_Khoa_hoc = BashOperator(task_id="load_dim_Khoa_hoc", bash_command="python ~/DWH_Cole_Project/src/load/Dim/Khoa_hoc.py")

    # Dim Lop_hoc
    extract_Lop_hoc=BashOperator(task_id="extract_dim_Lop_hoc", bash_command="python ~/DWH_Cole_Project/src/extract/Dim/Lop_hoc.py")
    transform_Lop_hoc = BashOperator(task_id="transform_dim_Lop_hoc", bash_command="python ~/DWH_Cole_Project/src/transform/Dim/Lop_hoc.py")
    load_Lop_hoc = BashOperator(task_id="load_dim_Lop_hoc", bash_command="python ~/DWH_Cole_Project/src/load/Dim/Lop_hoc.py")

    # Chạy nối tiếp từng nhóm
    extract_Saler >> transform_Saler >> load_Saler
    extract_Marketing >> transform_Marketing >> load_Marketing
    extract_Phan_Khoa_hoc >> transform_Phan_Khoa_hoc >> load_Phan_loai >> load_Khoa_hoc >> extract_Lop_hoc >> transform_Lop_hoc >> load_Lop_hoc
