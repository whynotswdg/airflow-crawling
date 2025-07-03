import json
import os
from datetime import datetime

# Airflow와 연동된 data 폴더의 절대 경로
AIRFLOW_DATA_DIR = "/opt/airflow/data"

def generate_timestamped_filename(prefix: str, extension: str = "json") -> str:
    """타임스탬프가 포함된 파일 이름을 생성합니다. (예: prefix_20250703_143000.json)"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
