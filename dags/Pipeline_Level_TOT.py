from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="Level_TOT_etl_pipeline",
    start_date=datetime(2025, 8, 16),
    schedule="0 6,12 * * *",
    catchup=False
) as dag:
    
    # Fact Count_Level_TOT
    extract_Count_Level_TOT = BashOperator(task_id="extract_fact_Count_Level_TOT", bash_command="python ~/DWH_Cole_Project/src/extract/Fact/Count_Level_TOT.py")
    transform_Count_Level_TOT = BashOperator(task_id="transform_fact_Count_Level_TOT", bash_command="python ~/DWH_Cole_Project/src/transform/Fact/Count_Level_TOT.py")
    load_Count_Level_TOT = BashOperator(task_id="load_fact_Count_Level_TOT", bash_command="python ~/DWH_Cole_Project/src/load/Fact/Count_Level_TOT.py")

    extract_Count_Level_TOT >> transform_Count_Level_TOT >> load_Count_Level_TOT