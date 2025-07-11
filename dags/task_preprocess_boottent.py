## task_preprocess_boottent.py
import json
import re
import calendar
from utils import save_json_data, generate_timestamped_filename, load_json_data

def preprocess_bootcamp_data(data, start_id=1):
    """
    raw 데이터를 테이블 정의서에 맞게 전처리합니다. (사용자 제공 로직)
    """
    processed_data = []
    current_id = start_id

    for item in data:
        date_range = item.get("학습기간", "")
        start_date_str = None
        end_date_str = None

        matches = re.search(r'(\d{2})\.(\d{2})\.(\d{2})\s*~\s*(\d{2})년\s*(\d{2})월중', date_range)
        if matches:
            start_year, start_month, start_day, end_year, end_month = matches.groups()
            start_date_str = f"20{start_year}-{start_month}-{start_day}"
            _, last_day = calendar.monthrange(int(f"20{end_year}"), int(end_month))
            end_date_str = f"20{end_year}-{end_month}-{last_day:02d}"
        else:
            matches = re.search(r'(\d{2})\.(\d{2})\.(\d{2})\s*~\s*(\d{2})\.(\d{2})\.(\d{2})', date_range)
            if matches:
                start_year, start_month, start_day, end_year, end_month, end_day = matches.groups()
                start_date_str = f"20{start_year}-{start_month}-{start_day}"
                end_date_str = f"20{end_year}-{end_month}-{end_day}"

        skills = [skill.strip() for skill in item.get("기술스택", "").split(',') if skill.strip()]
        skill_description_json = json.dumps(skills, ensure_ascii=False)

        processed_item = {
            "id": current_id,
            "name": item.get("교육과정명"),
            "type": "부트캠프",
            "skill_description": skill_description_json,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "status": item.get("모집상태")
        }
        processed_data.append(processed_item)
        current_id += 1

    return processed_data

def preprocess_and_save_data(ti):
    """
    Airflow Task: 이전 Task의 결과(JSON 파일)를 읽어 전처리하고,
    그 결과를 새로운 JSON 파일로 저장합니다.
    """
    # 1. 이전 scrape_boottent_task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    raw_data_path = ti.xcom_pull(task_ids='scrape_boottent_task', key='return_value')
    if not raw_data_path:
        raise ValueError("XCom으로부터 원본 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"전처리할 원본 데이터 파일을 로드합니다: {raw_data_path}")
    
    # 2. JSON 파일을 읽어 파이썬 리스트로 변환합니다.
    raw_data = load_json_data(raw_data_path)
    if not raw_data:
        print("전처리할 데이터가 없습니다.")
        return None

    # 3. 메인 전처리 함수를 호출합니다.
    preprocessed_data = preprocess_bootcamp_data(raw_data)
    
    # 4. 전처리된 최종 데이터를 새로운 JSON 파일로 저장합니다.
    if preprocessed_data:
        filename = generate_timestamped_filename("preprocessed_bootcamps")
        file_path = save_json_data(preprocessed_data, filename)
        print(f"전처리 완료! 총 {len(preprocessed_data)}개의 데이터를 {file_path} 파일로 저장했습니다.")
        return file_path # 다음 Task를 위해 결과 파일 경로를 반환합니다.
    else:
        print("전처리 후 데이터가 없습니다.")
        return None