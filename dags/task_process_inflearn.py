import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils import load_json_data, generate_timestamped_filename, save_json_data

def process_and_save_inflearn_data(ti):
    """
    Airflow Task: 스크래핑된 인프런 강의 데이터를 받아 전처리하고,
    그 결과를 PostgreSQL DB에 저장합니다.
    """
    # 1. 이전 scrape_inflearn_task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    input_path = ti.xcom_pull(task_ids='scrape_inflearn_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 스크래핑된 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"전처리할 원본 데이터 파일을 로드합니다: {input_path}")
    raw_data = load_json_data(input_path)
    if not raw_data:
        print("전처리할 데이터가 없습니다.")
        return

    # 2. DB 테이블 형식에 맞게 데이터 변환
    transformed_data = []
    for item in raw_data:
        # 'skills' 문자열을 리스트로 변환 후 다시 JSON 문자열로 변환
        skills_string = item.get('skills', '')
        skills_list = [skill.strip() for skill in skills_string.split(',') if skill.strip()]
        skills_json = json.dumps(skills_list, ensure_ascii=False)

        transformed_data.append({
            "name": item.get("title"),
            "type": "강의",
            "skill_description": skills_json,
            "company": item.get("instructor"),
            "price": item.get("price"),
            "url": item.get("link")
        })

    # (디버깅용) 변환된 데이터를 로컬에 JSON 파일로 저장
    debug_filename = generate_timestamped_filename("transformed_inflearn_courses")
    save_json_data(transformed_data, debug_filename)
    print(f"🔍 디버깅용 변환 데이터 저장 완료: /opt/airflow/data/{debug_filename}")

    # 3. Airflow Connection을 사용하여 DB에 연결
    hook = PostgresHook(postgres_conn_id='postgres_jobs_db')
    
    # 4. DB에 저장할 데이터(튜플 리스트)와 컬럼 준비
    # hook.insert_rows는 튜플의 리스트를 인자로 받습니다.
    rows_to_insert = [
        (
            item.get('name'),
            item.get('type'),
            item.get('skill_description'),
            item.get('company'),
            item.get('price'),
            item.get('url')
        ) for item in transformed_data
    ]
    target_fields = ["name", "type", "skill_description", "company", "price", "url"]

    # 5. 중복을 방지하며 데이터 삽입 (ON CONFLICT DO NOTHING)
    # hook.insert_rows는 ON CONFLICT를 직접 지원하지 않으므로, get_conn()을 사용합니다.
    conn = hook.get_conn()
    inserted_count = 0
    try:
        with conn.cursor() as cur:
            print("🔄 데이터 삽입/업데이트를 시작합니다...")
            for row in rows_to_insert:
                sql = """
                    INSERT INTO roadmaps (name, type, skill_description, company, price, url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                """
                cur.execute(sql, row)
                if cur.rowcount > 0:
                    inserted_count += 1
            conn.commit()
        print(f"✅ 작업 완료! 새롭게 추가된 데이터: {inserted_count}개")
    except Exception as e:
        print(f"🚨 DB 저장 중 오류 발생: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()