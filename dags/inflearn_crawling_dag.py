from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# 1단계에서 만든 task 파일에서 함수를 가져옵니다.
from task_scrape_inflearn import scrape_inflearn_courses

with DAG(
    dag_id="inflearn_crawling_dag",
    # 매일 새벽 3시에 실행되도록 설정
    schedule="0 3 * * *", 
    start_date=pendulum.datetime(2025, 7, 29, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 인프런 강의 정보 스크래핑 DAG
    - `scrape_inflearn_task`: 인프런 웹사이트에서 최신 IT 강의 목록을 스크래핑하여 JSON 파일로 저장합니다.
    """,
    tags=["crawling", "inflearn", "courses"],
) as dag:

    # 인프런 강의 정보를 스크래핑하는 단일 Task 정의
    scrape_inflearn_task = PythonOperator(
        task_id="scrape_inflearn_task",
        python_callable=scrape_inflearn_courses,
    )