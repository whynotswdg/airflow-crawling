from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Task 함수들을 임포트합니다.
from task_scrape_inflearn import scrape_inflearn_courses
from task_process_inflearn import process_and_save_inflearn_data

with DAG(
    dag_id="inflearn_crawling_dag",
    schedule="0 3 * * *", 
    start_date=pendulum.datetime(2025, 7, 29, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 인프런 강의 정보 수집 및 저장 파이프라인
    1. `scrape_inflearn_task`: 강의 원본 데이터 스크래핑
    2. `process_and_save_task`: 데이터 전처리 및 PostgreSQL에 저장
    """,
    tags=["crawling", "inflearn", "courses"],
) as dag:

    # Task 1: 원본 데이터 스크래핑
    scrape_inflearn_task = PythonOperator(
        task_id="scrape_inflearn_task",
        python_callable=scrape_inflearn_courses,
    )

    # Task 2: 전처리 및 PostgreSQL 저장
    process_and_save_task = PythonOperator(
        task_id="process_and_save_task",
        python_callable=process_and_save_inflearn_data,
        retries=3,  # DB 불안정성에 대비하여 3번 재시도
        retry_delay=timedelta(minutes=10), # 10분 간격
    )

    # Task 실행 순서 정의
    scrape_inflearn_task >> process_and_save_task