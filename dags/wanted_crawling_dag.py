from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# dags 폴더에 함께 있는 task 파일들에서 함수를 가져옵니다.
from task_extract_urls import extract_urls
from task_crawl_content import crawl_content
from task_save_to_db import save_data_to_mongodb
from task_preprocess_data import preprocess_data # 전처리 함수 임포트
from task_standardize_tech import standardize_tech_stacks # 표준화 함수 임포트

with DAG(
    dag_id="wanted_crawling_dag",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 7, 3, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 원티드 채용공고 데이터 파이프라인 (최종)
    1. `extract_urls_task`: URL 추출
    2. `crawl_content_task`: 상세 내용 크롤링
    3. `save_to_db_task`: 원본 데이터를 MongoDB에 저장 (병렬 실행)
    4. `preprocess_data_task`: LLM으로 1차 기술 스택 추출 (병렬 실행)
    5. `standardize_data_task`: 규칙 기반으로 2차 최종 표준화
    """,
    tags=["crawling", "wanted", "mongodb", "preprocessing", "standardization"],
) as dag:

    # Task 1: URL 추출
    extract_urls_task = PythonOperator(
        task_id="extract_urls_task",
        python_callable=extract_urls,
    )

    # Task 2: 상세 내용 크롤링
    crawl_content_task = PythonOperator(
        task_id="crawl_content_task",
        python_callable=crawl_content,
    )

    # Task 3: MongoDB에 저장
    save_to_db_task = PythonOperator(
        task_id="save_to_db_task",
        python_callable=save_data_to_mongodb,
    )

    # Task 4: LLM으로 기술스택 추출 전처리
    preprocess_data_task = PythonOperator(
        task_id="preprocess_data_task",
        python_callable=preprocess_data,
    )

    # Task 5: 최종 표준화 작업
    standardize_data_task = PythonOperator(
        task_id="standardize_data_task",
        python_callable=standardize_and_save_data,
    )

    # Task 실행 순서 정의
    extract_urls_task >> crawl_content_task

    # crawl_content_task가 끝나면, DB 저장과 1차 전처리를 동시에 진행
    crawl_content_task >> [save_to_db_task, preprocess_data_task]

    # 1차 전처리가 끝나면, 최종 표준화 작업 실행
    preprocess_data_task >> standardize_data_task
