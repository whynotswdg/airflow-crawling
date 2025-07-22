# airflow-crawling/dags/utils.py
import json
import os
import pendulum # pendulum 임포트
from datetime import timedelta
# Airflow와 연동된 data 폴더의 절대 경로
AIRFLOW_DATA_DIR = "/opt/airflow/data"

def generate_timestamped_filename(prefix: str, extension: str = "json") -> str:
    """타임스탬프가 포함된 파일 이름을 생성합니다. (예: prefix_20250708_011000.json)"""
    # 현재 시간을 KST(Asia/Seoul) 기준으로 가져옵니다.
    kst_time = pendulum.now("Asia/Seoul")
    timestamp = kst_time.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{timestamp}.{extension}"

def save_json_data(data: list | dict, filename: str) -> str:
    """주어진 데이터를 지정된 파일 이름으로 /opt/airflow/data 폴더에 JSON으로 저장합니다."""
    # 데이터 폴더가 없으면 생성
    os.makedirs(AIRFLOW_DATA_DIR, exist_ok=True)

    file_path = os.path.join(AIRFLOW_DATA_DIR, filename)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return file_path

def load_json_data(file_path: str) -> dict:
    """전체 파일 경로를 받아 JSON 데이터를 로드합니다."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# ===================================================================
# 클라우드를 통한 서버 배포 전 특정 시간 재시도 로직 (추후 EC2 배포시 제거 예정)
# ===================================================================

def get_retry_delay_at_9_10(context):
    """
    [공통 함수] 실패 시, 다음 날 오전 9시 10분에 재시도합니다.
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
    [공통 함수] 1차 재시도는 5분 후, 2차 재시도는 다음 날 오전 9시 11분에 수행합니다.
    """
    try_number = context["try_number"]
    
    if try_number <= 2:
        print("1차 재시도를 위해 5분 대기합니다.")
        return timedelta(minutes=5)
    else:
        print("2차 재시도를 위해 다음 날 오전 9시 11분까지 대기합니다.")
        now = pendulum.now("Asia/Seoul")
        target_time = now.replace(hour=9, minute=11, second=0, microsecond=0)

        if now >= target_time:
            target_time = target_time.add(days=1)

        delay = target_time - now
        return delay