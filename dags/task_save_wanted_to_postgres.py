import json
import pandas as pd
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import load_json_data

def process_and_send_to_postgres(ti):
    """
    Airflow Task: 여러 이전 Task들의 결과(JSON 파일)를 XCom으로 받아
    최종 데이터를 가공한 후 PostgreSQL DB에 저장합니다.
    """
    # 1. 이전 Task들로부터 파일 경로를 XCom으로 받아옵니다.
    clustered_path = ti.xcom_pull(task_ids='clustering_jobs_task', key='return_value')
    keyword_path = ti.xcom_pull(task_ids='tokenize_jobs_task', key='return_value')

    if not clustered_path or not keyword_path:
        raise ValueError("XCom으로부터 클러스터링 또는 토큰화 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"클러스터링 데이터 로드: {clustered_path}")
    print(f"키워드 데이터 로드: {keyword_path}")

    clustered_data = pd.DataFrame(load_json_data(clustered_path))
    keyword_data = pd.DataFrame(load_json_data(keyword_path))

    # 2. Airflow Connection을 사용하여 DB 엔진 생성
    hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
    engine = hook.get_sqlalchemy_engine()

    # 3. DB에서 job_required_skills 테이블 데이터 가져오기
    try:
        # [수정] Connection 대신 engine 객체를 직접 전달합니다.
        job_required_skills = pd.read_sql("SELECT id, job_name FROM job_required_skills", engine)
        print(f"✅ DB에서 {len(job_required_skills)}개의 직무 카테고리를 가져왔습니다.")
    except Exception as e:
        print(f"🚨 DB에서 'job_required_skills' 테이블을 읽는 중 오류 발생: {e}")
        raise
    
    # --- 데이터 전처리 시작 ---
    print("데이터 병합 및 전처리를 시작합니다...")
    # 4. clustered_data와 keyword_data 병합
    merged_data = clustered_data.merge(keyword_data, on='id', how='left')

    # 5. 직무 카테고리 ID 조인
    job_required_skills.rename(columns={"id": "job_required_skill_id", 'job_name': 'job_category'}, inplace=True)
    join_data = merged_data.merge(job_required_skills[["job_category", "job_required_skill_id"]], on='job_category', how='left')

    # 6. 최종 데이터프레임 생성 및 후처리
    join_data.drop(columns=["job_category", "cluster"], inplace=True, errors="ignore")
    
    for col in ["required_skills", "preferred_skills", "main_tasks_skills"]:
        # NaN 값을 빈 리스트로 처리 후 JSON 문자열로 변환
        join_data[col] = join_data[col].apply(lambda x: json.dumps(x if isinstance(x, list) else []))

    join_data["address"] = join_data["address"].fillna("")
    
    # DB 테이블 컬럼 순서에 맞게 재정렬 (embedding 컬럼은 제외하고 로드)
    target_columns = [
        'id', 'title', 'company_name', 'size', 'address', 'job_required_skill_id',
        'employment_type', 'applicant_type', 'posting_date', 'deadline',
        'main_tasks', 'qualifications', 'preferences', 'tech_stack',
        'required_skills', 'preferred_skills', 'main_tasks_skills'
    ]
    # 'created_at'은 DB에서 자동으로 생성되므로 여기서는 제외합니다.
    final_data = join_data[[col for col in target_columns if col in join_data.columns]]

    # 7. DB에 이미 있는 ID는 제외하여 중복 방지
    try:
        with engine.connect() as conn:
            existing_ids = pd.read_sql(text("SELECT id FROM job_posts"), conn)
            existing_ids_set = set(existing_ids['id'])
        
        mask = ~final_data['id'].isin(existing_ids_set)
        new_data_to_insert = final_data[mask]
        print(f"기존 데이터와 비교 후, {len(new_data_to_insert)}개의 새로운 데이터를 저장합니다.")
    except Exception as e:
        print(f"🚨 DB에서 기존 ID를 확인하는 중 오류 발생: {e}")
        new_data_to_insert = final_data # 오류 시 전체 데이터를 저장 시도

    if new_data_to_insert.empty:
        print("새롭게 추가할 데이터가 없습니다.")
        return

    # 8. 최종 데이터를 PostgreSQL 'job_posts' 테이블에 저장
    try:
        new_data_to_insert.to_sql('job_posts', engine, if_exists='append', index=False)
        print(f"🎉 성공! {len(new_data_to_insert)}개의 데이터가 'job_posts' 테이블에 저장되었습니다.")
    except Exception as e:
        print(f"🚨 최종 데이터를 DB에 저장하는 중 오류 발생: {e}")
        raise