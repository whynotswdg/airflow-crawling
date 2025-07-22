import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
# [1] 파일 저장을 위해 utils에서 필요한 함수들을 추가로 import 합니다.
from utils import load_json_data, save_json_data, generate_timestamped_filename

def process_and_send_to_postgres(ti):
    """
    Airflow Task: 여러 이전 Task들의 결과(JSON 파일)를 XCom으로 받아
    최종 데이터를 가공한 후, JSON 파일로 저장하고 PostgreSQL DB에도 전송합니다.
    """
    # --- 데이터 로드 및 전처리 (이전과 동일) ---
    clustered_path = ti.xcom_pull(task_ids='clustering_jobs_task', key='return_value')
    keyword_path = ti.xcom_pull(task_ids='tokenize_jobs_task', key='return_value')

    if not clustered_path or not keyword_path:
        raise ValueError("XCom으로부터 클러스터링 또는 토큰화 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"클러스터링 데이터 로드: {clustered_path}")
    print(f"키워드 데이터 로드: {keyword_path}")

    clustered_data = pd.DataFrame(load_json_data(clustered_path))
    keyword_data = pd.DataFrame(load_json_data(keyword_path))

    hook = PostgresHook(postgres_conn_id='postgres_jobs_db')

    try:
        sql = "SELECT id, job_name FROM job_required_skills"
        job_required_skills = hook.get_pandas_df(sql=sql)
        print(f"✅ DB에서 {len(job_required_skills)}개의 직무 카테고리를 가져왔습니다.")
    except Exception as e:
        print(f"🚨 DB에서 'job_required_skills' 테이블을 읽는 중 오류 발생: {e}")
        raise

    print("데이터 병합 및 전처리를 시작합니다...")
    merged_data = clustered_data.merge(keyword_data, on='id', how='left')
    job_required_skills.rename(columns={"id": "job_required_skill_id", 'job_name': 'representative_category'}, inplace=True)
    join_data = merged_data.merge(job_required_skills[["representative_category", "job_required_skill_id"]], on='representative_category', how='left')
    join_data.drop(columns=["representative_category", "job_category", "cluster"], inplace=True, errors="ignore")
    
    for col in ["required_skills", "preferred_skills", "main_tasks_skills"]:
        join_data[col] = join_data[col].apply(lambda x: json.dumps(x if isinstance(x, list) else []))
    join_data["address"] = join_data["address"].fillna("")
    
    target_columns = [
        'id', 'title', 'company_name', 'size', 'address', 'job_required_skill_id',
        'employment_type', 'applicant_type', 'posting_date', 'deadline',
        'main_tasks', 'qualifications', 'preferences', 'tech_stack',
        'required_skills', 'preferred_skills', 'main_tasks_skills'
    ]
    final_data = join_data[[col for col in target_columns if col in join_data.columns]]

    # --- [2] (선택사항) DB 전송 전, 최종 데이터를 로컬에 JSON 파일로 저장 ---
    # DataFrame을 list of dict 형태로 변환
    final_data_list = final_data.to_dict(orient='records')
    # 파일로 저장
    debug_filename = generate_timestamped_filename("final_postgres_payload")
    save_json_data(final_data_list, debug_filename)
    print(f"🔍 디버깅용 최종 데이터 저장 완료: /opt/airflow/data/{debug_filename}")


    # --- DB 저장 로직 (이전과 동일) ---
    try:
        existing_ids_df = hook.get_pandas_df(sql="SELECT id FROM job_posts")
        if not existing_ids_df.empty:
            existing_ids_set = set(existing_ids_df['id'])
            mask = ~final_data['id'].isin(existing_ids_set)
            new_data_to_insert = final_data[mask]
        else:
            new_data_to_insert = final_data
            
        print(f"기존 데이터와 비교 후, {len(new_data_to_insert)}개의 새로운 데이터를 저장합니다.")
    except Exception as e:
        print(f"🚨 DB에서 기존 ID를 확인하는 중 오류 발생: {e}. 모든 데이터를 저장 시도합니다.")
        new_data_to_insert = final_data

    if new_data_to_insert.empty:
        print("새롭게 추가할 데이터가 없습니다.")
        return

    try:
        rows_to_insert = list(new_data_to_insert.itertuples(index=False, name=None))
        target_fields = list(new_data_to_insert.columns)
        
        hook.insert_rows(
            table="job_posts",
            rows=rows_to_insert,
            target_fields=target_fields,
            commit_every=1000
        )
        print(f"🎉 성공! {len(new_data_to_insert)}개의 데이터가 'job_posts' 테이블에 저장되었습니다.")
    except Exception as e:
        print(f"🚨 최종 데이터를 DB에 저장하는 중 오류 발생: {e}")
        raise