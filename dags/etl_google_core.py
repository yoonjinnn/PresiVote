import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from etl_google_base import *

with DAG(
    dag_id="google_trends_keyword_to_s3_final",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    tags=["trends", "data_ingestion", "s3"]
) as dag:
    fetch_and_upload_task = PythonOperator(
        task_id="fetch_and_upload_election_trends_data",
        python_callable=fetch_and_upload_trends_data,
        op_kwargs={
            "keyword_list":  [
                "이재명","김문수","이준석"
            ],
            "timeframe": "today 1-m",
            "geo": "KR"
        },
        provide_context=True
    )