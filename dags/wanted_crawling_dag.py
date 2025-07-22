from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta 
# dags 폴더에 함께 있는 task 파일들에서 함수를 가져옵니다.
from task_extract_urls import extract_urls
from task_crawl_content import crawl_content
from task_save_to_db import save_data_to_mongodb
from task_preprocess_data import preprocess_data 
from task_standardize_tech import standardize_and_save_data 
from task_tokenize_jobs import tokenize_and_post_process_jobs
from task_embedding_jobs import embed_jobs_data
from task_clustering_jobs import cluster_jobs_data
from task_save_wanted_to_postgres import process_and_send_to_postgres

# ===================================================================
# 클라우드를 통한 서버 배포 전 특정 시간 재시도 로직 (추후 EC2 배포시 제거 예정)
# ===================================================================

def get_retry_delay_at_9_10(context):
    """
    [MongoDB 저장용] 실패 시, 다음 날 오전 9시 10분에 재시도합니다.
    """
    print("재시도를 위해 다음 날 오전 9시 10분까지 대기합니다.")
    now = pendulum.now("Asia/Seoul")
    target_time = now.replace(hour=9, minute=10, second=0, microsecond=0)

    # 만약 현재 시간이 이미 오전 9시 10분을 지났다면, 다음 날 9시 10분으로 설정
    if now >= target_time:
        target_time = target_time.add(days=1)

    delay = target_time - now
    return delay

def get_retry_delay_for_postgres(context):
    """
    [PostgreSQL 저장용] 1차 재시도는 5분 후, 2차 재시도는 다음 날 오전 9시 11분에 수행합니다.
    """
    try_number = context["try_number"]
    
    if try_number <= 2: # 1차 재시도
        print("1차 재시도를 위해 5분 대기합니다.")
        return timedelta(minutes=5)
    else: # 2차 재시도
        print("2차 재시도를 위해 다음 날 오전 9시 11분까지 대기합니다.")
        now = pendulum.now("Asia/Seoul")
        target_time = now.replace(hour=9, minute=11, second=0, microsecond=0)

        if now >= target_time:
            target_time = target_time.add(days=1)

        delay = target_time - now
        return delay
# ===================================================================

with DAG(
    dag_id="wanted_crawling_dag",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 7, 3, tz="Asia/Seoul"),
    catchup=False,
    doc_md="""
    ### 원티드 채용공고 데이터 파이프라인 (분석 전체)
    1.  **URL 추출**: `extract_urls_task`
    2.  **상세 내용 크롤링**: `crawl_content_task`
    3.  **병렬 처리**:
        - **MongoDB 저장**: `save_to_db_task`
        - **1차 전처리 (LLM 기술스택)**: `preprocess_data_task`
    4.  **2차 표준화**: `standardize_data_task`
    5.  **병렬 처리**:
        - **토큰화 (LLM 키워드)**: `tokenize_jobs_task`
        - **임베딩**: `embedding_jobs_task`
    6.  **클러스터링**: `clustering_jobs_task`
    7.  **최종 PostgreSQL 저장**: `save_to_postgres_task`
    """,
    tags=["crawling", "wanted", "mongodb", "preprocessing", "standardization", "embedding", "clustering"],
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
        retries=1, # 1번 재시도
        retry_delay=get_retry_delay_at_9_10, # 9시 10분 재시도 함수 연결
    )


    # Task 4: LLM으로 기술스택 추출 전처리
    preprocess_data_task = PythonOperator(
        task_id="preprocess_data_task",
        python_callable=preprocess_data,
    )

    # Task 5: 기술스택 표준화 후처리
    standardize_data_task = PythonOperator(
        task_id="standardize_data_task",
        python_callable=standardize_and_save_data,
    )

        # Task 6: LLM으로 본문 섹션별 토큰화 및 후처리
    tokenize_jobs_task = PythonOperator(
        task_id="tokenize_jobs_task",
        python_callable=tokenize_and_post_process_jobs,
    )

        # Task 7 : 표준화 후처리 된 공고 임베딩
    embedding_jobs_task = PythonOperator(
        task_id="embedding_jobs_task",
        python_callable=embed_jobs_data,
    )

    clustering_jobs_task = PythonOperator(
        task_id="clustering_jobs_task",
        python_callable=cluster_jobs_data)
    
    save_to_postgres_task = PythonOperator(
        task_id="save_to_postgres_task",
        python_callable=process_and_send_to_postgres,
        retries=2,
        retry_delay=get_retry_delay_for_postgres, # 9시 11분 재시도 함수 연결
    )

    # --- 최종 Task 실행 순서 정의 ---
    # 1. 크롤링 파트
    extract_urls_task >> crawl_content_task

    # 2. 크롤링이 완료되면 DB 저장, 기술스택 추출, 본문 토큰화를 병렬로 진행
    crawl_content_task >> [save_to_db_task, preprocess_data_task, tokenize_jobs_task]
    
    # 3. 기술스택 추출 -> 표준화 -> 임베딩 -> 클러스터링 순으로 진행
    preprocess_data_task >> standardize_data_task >> embedding_jobs_task >> clustering_jobs_task
    
    # 4. 클러스터링과 토큰화가 모두 끝나면, 최종적으로 PostgreSQL에 저장
    [clustering_jobs_task, tokenize_jobs_task] >> save_to_postgres_task