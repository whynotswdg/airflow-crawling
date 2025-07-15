import json
import psycopg2
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook

def save_data_to_db(ti):
    """
    Airflow Task: 이전 Task의 결과(JSON 파일)를 XCom으로 받아
    PostgreSQL DB에 저장합니다.
    """
    # 1. 이전 전처리 Task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    preprocessed_path = ti.xcom_pull(task_ids='preprocess_boottent_task', key='return_value')
    if not preprocessed_path:
        raise ValueError("XCom으로부터 전처리된 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"✅ DB에 저장할 데이터 파일을 로드합니다: {preprocessed_path}")
    with open(preprocessed_path, 'r', encoding='utf-8') as f:
        data_to_save = json.load(f)

    if not data_to_save:
        print("DB에 저장할 데이터가 없습니다.")
        return

    # 2. Airflow Connection을 사용하여 DB에 연결합니다.
    #    (Connection ID: 'postgres_jobs_db')
    hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # 3. 데이터를 DB에 삽입(INSERT)합니다.
    #    - ON CONFLICT 구문을 사용하여 id가 중복될 경우 UPDATE를 수행합니다 (Upsert).
    #      이렇게 하면 DAG을 여러 번 실행해도 데이터가 중복되지 않습니다.
    
    # 테이블 컬럼 순서에 맞게 데이터 준비
    columns = data_to_save[0].keys()
    values = [[row[col] for col in columns] for row in data_to_save]

    # 제외할 컬럼 (id는 중복 검사에만 사용)
    update_columns = [col for col in columns if col != 'id']
    
    # SQL 쿼리 생성
    insert_sql = f"""
        INSERT INTO roadmaps ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])}
    """
    
    try:
        # execute_values를 사용한 효율적인 대량 INSERT
        execute_values(cur, insert_sql, values)
        conn.commit()
        print(f"✅ 성공: 총 {len(data_to_save)}개의 데이터가 DB에 저장(업데이트)되었습니다.")
        
    except Exception as e:
        print(f"❌ DB 저장 중 오류 발생: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()