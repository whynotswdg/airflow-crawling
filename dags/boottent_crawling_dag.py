from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# dags 폴더에 있는 task 파일들에서 함수를 가져옵니다.
from task_scrape_boottent import scrape_boottent_data
from task_preprocess_boottent import preprocess_and_save_data # 전처리 함수 임포트

with DAG(
    dag_id="boottent_crawling_dag",
    schedule="0 3 * * *", 
    start_date=pendulum.datetime(2025, 7, 12, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 부트텐트 부트캠프 정보 파이프라인
    1. `scrape_boottent_task`: 부트캠프 원본 데이터 스크래핑 후 JSON 파일로 저장
    2. `preprocess_boottent_task`: 원본 데이터를 전처리하여 새로운 JSON 파일로 저장
    """,
    tags=["crawling", "boottent", "bootcamp", "preprocessing"],
) as dag:

    # Task 1: 원본 데이터 스크래핑
    scrape_boottent_task = PythonOperator(
        task_id="scrape_boottent_task",
        python_callable=scrape_boottent_data,
    )

    # Task 2: 데이터 전처리 (새로 추가)
    preprocess_boottent_task = PythonOperator(
        task_id="preprocess_boottent_task",
        python_callable=preprocess_and_save_data,
    )

    # Task 실행 순서 정의: 스크래핑 >> 전처리
    scrape_boottent_task >> preprocess_boottent_task