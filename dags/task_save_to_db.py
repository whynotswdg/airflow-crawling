import json
from airflow.providers.mongo.hooks.mongo import MongoHook

def load_json_from_path(file_path: str) -> list | None:
    """지정된 경로의 JSON 파일을 읽어 리스트 형태로 반환합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # JSON 파일의 최상위가 리스트라고 가정합니다.
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
        print(f"오류: JSON 파일을 읽거나 파싱하는 데 실패했습니다: {e}")
        return None

def save_data_to_mongodb(ti):
    """
    이전 Task에서 XCom으로 전달받은 JSON 파일 경로를 읽어
    그 안의 데이터를 MongoDB에 저장합니다.
    """
    # 1. crawl_content_task가 XCom으로 넘겨준 최종 파일 경로를 가져옵니다.
    json_file_path = ti.xcom_pull(task_ids='crawl_content_task', key='return_value')
    if not json_file_path:
        raise ValueError("XCom으로부터 파일 경로를 가져오지 못했습니다. 이전 Task를 확인해주세요.")

    print(f"MongoDB에 저장할 데이터 파일을 로드합니다: {json_file_path}")
    
    # 2. 파일 경로를 이용해 JSON 데이터를 파이썬 리스트로 변환합니다.
    data_to_save = load_json_from_path(json_file_path)
    if not data_to_save or not isinstance(data_to_save, list):
        print("저장할 데이터가 없거나 데이터 형식이 올바르지 않습니다.")
        return # 오류 대신 정상 종료로 처리하여 DAG 전체가 실패하지 않도록 함

    # 3. Airflow Connection에 저장된 'mongo_default' 접속 정보를 사용합니다.
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    
    # Connection 정보에 지정한 DB 이름(Schema)과 사용할 Collection 이름을 설정합니다.
    db = client[hook.schema] 
    collection = db.wanted_jobs # "wanted_jobs" 라는 이름의 테이블(Collection)에 저장

    try:
        # 4. MongoDB에 여러 데이터를 한 번에 저장(insert)합니다.
        print(f"총 {len(data_to_save)}개의 데이터를 MongoDB에 저장하기 시작합니다...")
        # id를 기준으로 중복 저장을 방지하려면, update_one과 upsert=True를 사용해야 합니다.
        # 여기서는 단순 insert_many를 사용하여 매번 새로 저장합니다.
        collection.insert_many(data_to_save)
        print("MongoDB 저장 완료!")
    except Exception as e:
        print(f"MongoDB 저장 중 오류 발생: {e}")
        raise # 오류 발생 시 Airflow Task를 실패 처리
    finally:
        # 5. 작업 완료 후에는 항상 접속을 종료합니다.
        client.close()