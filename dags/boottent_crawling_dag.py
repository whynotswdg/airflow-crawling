from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# dags 폴더에 새로 만든 task 파일에서 함수를 가져옵니다.
from task_scrape_boottent import scrape_boottent_data

with DAG(
    dag_id="boottent_crawling_dag",
    # 다른 DAG와 겹치지 않도록 새벽 3시에 실행하도록 설정
    schedule="0 3 * * *", 
    start_date=pendulum.datetime(2025, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 부트텐트 부트캠프 정보 스크래핑 DAG
    - `scrape_boottent_task`: 부트텐트 웹사이트에서 부트캠프 목록 전체를 스크래핑하여 JSON 파일로 저장합니다.
    """,
    tags=["crawling", "boottent", "bootcamp"],
) as dag:

    # 부트캠프 정보를 스크래핑하는 단일 Task 정의
    scrape_boottent_task = PythonOperator(
        task_id="scrape_boottent_task",
        python_callable=scrape_boottent_data,
    )