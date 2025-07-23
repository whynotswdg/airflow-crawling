from utils import save_json_data, generate_timestamped_filename, load_json_data
# 1단계에서 만든 공통 모듈을 임포트합니다.
from tech_standardizer import get_tech_stack_map, standardize_tech_list

def standardize_and_save_data(ti):
    """
    Airflow Task: LLM이 처리한 JSON 파일을 읽어 기술 스택을 최종 표준화하고,
    그 결과를 새로운 JSON 파일로 저장합니다.
    """
    input_path = ti.xcom_pull(task_ids='preprocess_data_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 이전 단계의 파일 경로를 가져오지 못했습니다.")

    print(f"최종 표준화할 데이터 파일을 로드합니다: {input_path}")
    data_to_process = load_json_data(input_path)
    if not data_to_process:
        print("표준화할 데이터가 없습니다.")
        return None

    tech_map = get_tech_stack_map()
    final_data = []

    for job in data_to_process:
        raw_tech_stack_str = job.get("tech_stack")
        
        # 문자열을 리스트로 변환하여 공통 함수에 전달
        raw_list = [stack.strip() for stack in raw_tech_stack_str.split(',')] if raw_tech_stack_str else []
        standardized_list = standardize_tech_list(raw_list, tech_map)
        
        # 다시 쉼표로 구분된 문자열로 변환하여 저장
        job["tech_stack"] = ", ".join(standardized_list)
        final_data.append(job)

    filename = generate_timestamped_filename("final_wanted_jobs")
    file_path = save_json_data(final_data, filename)
    print(f"최종 표준화 완료! 데이터를 {file_path} 파일로 저장했습니다.")
    return file_path