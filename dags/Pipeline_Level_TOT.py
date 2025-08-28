from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone

local_tz = timezone("Asia/Ho_Chi_Minh")

# Hàm tiện ích để log stderr + task_id + timestamp
def bash_with_error_log(python_file: str, task_id: str) -> str:
    return (
        f'bash -c \'python {python_file} 2>&1 | while read line; '
        f'do echo "[`date "+%Y-%m-%d %H:%M:%S"`][{task_id}] $line" >> ~/DWH_Cole_Project/log_error.txt; done\''
    )

with DAG(
    dag_id="Level_TOT_etl_pipeline",
    start_date=datetime(2025, 8, 16, tzinfo=local_tz),
    schedule="0 6,12 * * *",
    catchup=False
) as dag:

    # Fact Count_Level_TOT
    extract_Count_Level_TOT = BashOperator(
        task_id="extract_fact_Count_Level_TOT",
        bash_command=bash_with_error_log(
            "~/DWH_Cole_Project/src/extract/Fact/Count_Level_TOT.py",
            "extract_fact_Count_Level_TOT"
        )
    )

    transform_Count_Level_TOT = BashOperator(
        task_id="transform_fact_Count_Level_TOT",
        bash_command=bash_with_error_log(
            "~/DWH_Cole_Project/src/transform/Fact/Count_Level_TOT.py",
            "transform_fact_Count_Level_TOT"
        )
    )

    load_Count_Level_TOT = BashOperator(
        task_id="load_fact_Count_Level_TOT",
        bash_command=bash_with_error_log(
            "~/DWH_Cole_Project/src/load/Fact/Count_Level_TOT.py",
            "load_fact_Count_Level_TOT"
        )
    )

    extract_Count_Level_TOT >> transform_Count_Level_TOT >> load_Count_Level_TOT
