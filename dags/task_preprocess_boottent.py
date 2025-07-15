import json
import pandas as pd

# Airflow 환경에 맞게 utils 함수를 임포트합니다.
from utils import save_json_data, generate_timestamped_filename, load_json_data

# ✅ DAG 파일이 찾는 이름인 'preprocess_and_save_data'로 함수 이름을 변경했습니다.
def preprocess_and_save_data(ti):
    """
    Airflow Task: 이전 Task의 결과(JSON 파일)를 XCom으로 받아 전처리하고,
    그 결과를 DB에 맞는 최종 JSON 파일로 저장합니다.
    """
    # 1. 이전 스크래핑 Task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    raw_data_path = ti.xcom_pull(task_ids='scrape_boottent_task', key='return_value')
    if not raw_data_path:
        raise ValueError("XCom으로부터 원본 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"✅ 원본 데이터 파일을 로드합니다: {raw_data_path}")
    raw_data = load_json_data(raw_data_path)
    if not raw_data:
        print("전처리할 데이터가 없습니다.")
        return None
    
    # 2. 로드한 데이터를 pandas DataFrame으로 변환합니다.
    df = pd.json_normalize(raw_data)
    print(f"로드 완료: 총 {len(df)}개의 원본 데이터")

    # 3. 최종 테이블 컬럼에 맞게 데이터를 선택하고 이름을 변경합니다.
    column_mapping = {
        'course_name': 'name',
        'tech_stack': 'skill_description',
        'company': 'company',
        'program_course': 'program_course',
        'start_date': 'start_date',
        'end_date': 'end_date',
        'deadline': 'deadline',
        'location': 'location',
        'onoff': 'onoff',
        'participation_time': 'participation_time',
        'status': 'status'
    }
    
    source_columns = [col for col in column_mapping.keys() if col in df.columns]
    df_processed = df[source_columns].copy()
    df_processed.rename(columns=column_mapping, inplace=True)

    # 4. 데이터를 변환하고 새로운 컬럼을 추가합니다.
    df_processed.insert(0, 'id', range(1, len(df_processed) + 1))
    df_processed['type'] = '부트캠프'
    df_processed['skill_description'] = df_processed['skill_description'].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) and x else '[]'
    )

    # 5. 데이터를 정제합니다.
    required_columns = ['name', 'start_date']
    original_count = len(df_processed)
    df_processed.dropna(subset=required_columns, inplace=True)
    
    if original_count > len(df_processed):
        print(f"🧹 필수 값이 없어 {original_count - len(df_processed)}개의 데이터가 삭제되었습니다.")

    # 6. 최종 컬럼 순서를 정의하고 적용합니다.
    final_columns_order = [
        'id', 'name', 'type', 'skill_description', 'company', 'program_course',
        'start_date', 'end_date', 'deadline', 'location', 'onoff',
        'participation_time', 'status'
    ]
    final_columns_in_df = [col for col in final_columns_order if col in df_processed.columns]
    df_final = df_processed[final_columns_in_df]

    # 7. 전처리된 최종 데이터를 새로운 JSON 파일로 저장합니다.
    if not df_final.empty:
        preprocessed_data = df_final.to_dict(orient='records')
        filename = generate_timestamped_filename("preprocessed_bootcamps_final")
        file_path = save_json_data(preprocessed_data, filename)
        
        print(f"✅ 전처리 완료! 총 {len(preprocessed_data)}개의 데이터를 다음 경로에 저장했습니다: {file_path}")
        
        return file_path
    else:
        print("전처리 후 데이터가 없습니다.")
        return None