from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# dags 폴더에 함께 있는 task 파일들에서 함수를 가져옵니다.
from task_extract_urls import extract_urls
from task_crawl_content import crawl_content

with DAG(
    dag_id="wanted_crawling_dag",
    schedule="0 1 * * *",  # 매일 새벽 1시에 실행 (KST 기준 오전 10시)
    start_date=pendulum.datetime(2025, 7, 3, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 원티드 채용공고 크롤링 DAG
    1. `extract_urls_task`: 원티드 채용공고 목록에서 상세 페이지 URL을 추출하여 JSON 파일로 저장합니다.
    2. `crawl_content_task`: 위에서 만든 JSON 파일을 읽어 각 URL을 방문하고, 상세 내용을 파싱하여 최종 JSON 파일로 저장합니다.
    """,
    tags=["crawling", "wanted"],
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

    # Task 실행 순서 정의: URL 추출이 끝나면 상세 내용 크롤링을 시작
    extract_urls_task >> crawl_content_task
