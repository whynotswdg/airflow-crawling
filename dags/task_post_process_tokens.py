from utils import save_json_data, generate_timestamped_filename, load_json_data
# 1단계에서 만든 공통 모듈을 임포트합니다.
from tech_standardizer import get_tech_stack_map, standardize_tech_list

def post_process_tokenized_data(ti):
    """
    Airflow Task: 토큰화된 JSON 파일을 읽어, 각 키워드 리스트를 표준화하고,
    그 결과를 새로운 JSON 파일로 저장합니다.
    """
    # 1. 이전 tokenize_jobs_task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    input_path = ti.xcom_pull(task_ids='tokenize_jobs_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 토큰화된 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"토큰 후처리를 위해 데이터 파일을 로드합니다: {input_path}")
    data_to_process = load_json_data(input_path)
    if not data_to_process:
        print("후처리할 데이터가 없습니다.")
        return None

    tech_map = get_tech_stack_map()
    final_data = []
    
    print(f"총 {len(data_to_process)}개 데이터의 토큰 표준화를 시작합니다...")
    for job in data_to_process:
        processed_job = {'id': job.get('id')}
        
        # 2. 각 스킬 섹션별로 표준화 함수를 적용합니다.
        for key in ['main_tasks_skills', 'required_skills', 'preferred_skills']:
            raw_keyword_list = job.get(key, [])
            # standardize_tech_list 함수를 사용하여 리스트를 직접 표준화합니다.
            standardized_list = standardize_tech_list(raw_keyword_list, tech_map)
            processed_job[key] = standardized_list
            
        final_data.append(processed_job)

    # 3. 최종 결과를 새로운 JSON 파일로 저장합니다.
    filename = generate_timestamped_filename("post_processed_tokens")
    file_path = save_json_data(final_data, filename)
    print(f"토큰 후처리 완료! 데이터를 새 파일로 저장했습니다: {file_path}")
    
    return file_path