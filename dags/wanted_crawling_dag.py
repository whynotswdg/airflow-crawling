from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# dags 폴더에 함께 있는 task 파일들에서 함수를 가져옵니다.
from task_extract_urls import extract_urls
from task_crawl_content import crawl_content
from task_save_to_db import save_data_to_mongodb # DB 저장 함수 임포트

with DAG(
    dag_id="wanted_crawling_dag",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 7, 3, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 원티드 채용공고 크롤링 및 DB 저장 DAG
    1. `extract_urls_task`: URL 추출 후 JSON 파일로 저장
    2. `crawl_content_task`: URL 목록으로 상세 내용 크롤링 후 최종 JSON 파일로 저장
    3. `save_to_db_task`: 최종 JSON 파일의 데이터를 MongoDB에 저장
    """,
    tags=["crawling", "wanted", "mongodb"],
) as dag:

    # 첫 번째 Task: URL 추출
    extract_urls_task = PythonOperator(
        task_id="extract_urls_task",
        python_callable=extract_urls,
    )

    # 두 번째 Task: 상세 내용 크롤링
    crawl_content_task = PythonOperator(
        task_id="crawl_content_task",
        python_callable=crawl_content,
    )

    # 세 번째 Task: MongoDB에 저장 (새로 추가)
    save_to_db_task = PythonOperator(
        task_id="save_to_db_task",
        python_callable=save_data_to_mongodb,
    )

    # Task 실행 순서 정의: URL 추출 >> 상세 내용 크롤링 >> DB 저장
    extract_urls_task >> crawl_content_task >> save_to_db_task