import json
from airflow.providers.mongo.hooks.mongo import MongoHook
# 네트워크/연결 관련 예외를 명시적으로 처리하기 위해 import 합니다.
from pymongo.errors import ConnectionFailure

def load_json_from_path(file_path: str) -> list | None:
    """지정된 경로의 JSON 파일을 읽어 리스트 형태로 반환합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
        print(f"오류: JSON 파일을 읽거나 파싱하는 데 실패했습니다: {e}")
        return None

def save_data_to_mongodb(ti):
    """
    XCom에서 JSON 파일 경로를 가져와 MongoDB에 데이터를 저장합니다.
    실패 시 Airflow의 재시도 메커니즘을 사용합니다.
    """
    # 1. 이전 Task에서 파일 경로 가져오기
    json_file_path = ti.xcom_pull(task_ids='crawl_content_task', key='return_value')
    if not json_file_path:
        raise ValueError("XCom으로부터 파일 경로를 가져오지 못했습니다. 이전 Task를 확인해주세요.")

    print(f"MongoDB에 저장할 데이터 파일을 로드합니다: {json_file_path}")
    
    # 2. JSON 데이터 로드
    data_to_save = load_json_from_path(json_file_path)
    if not data_to_save or not isinstance(data_to_save, list):
        print("저장할 데이터가 없거나 데이터 형식이 올바르지 않습니다. Task를 성공으로 간주하고 건너뜁니다.")
        # 데이터가 없는 것은 '실패'가 아니므로, DAG이 멈추지 않도록 파일 경로를 그대로 반환합니다.
        return json_file_path

    # MongoDB 연결 및 저장을 위한 client 변수 초기화
    client = None
    try:
        # 3. MongoDB 연결
        print("MongoDB에 연결을 시도합니다...")
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        
        # 서버가 살아있는지 간단히 확인 (ping)
        client.admin.command('ping')
        print("MongoDB 연결에 성공했습니다.")
        
        db = client.get_default_database() 
        collection = db.wanted_jobs

        # 4. 데이터 저장
        print(f"총 {len(data_to_save)}개의 데이터를 MongoDB에 저장하기 시작합니다...")
        collection.insert_many(data_to_save)
        print("MongoDB 저장 완료!")

    # 가장 흔한 예외인 ConnectionFailure를 먼저 처리합니다.
    except ConnectionFailure as e:
        print(f"MongoDB 연결 실패 (Airflow가 재시도합니다): {e}")
        # 예외를 다시 발생시켜 Airflow가 실패를 인지하고 재시도하도록 합니다.
        raise e
    except Exception as e:
        # 그 외 데이터 저장 중 발생할 수 있는 모든 예외 처리
        print(f"MongoDB 저장 중 오류 발생 (Airflow가 재시도합니다): {e}")
        raise e
    finally:
        # 5. client가 성공적으로 생성되었을 경우에만 연결을 종료합니다.
        if client:
            client.close()
            print("MongoDB 연결을 종료했습니다.")
            
    return json_file_path
