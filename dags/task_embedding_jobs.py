import json
import numpy as np
from sentence_transformers import SentenceTransformer
from utils import save_json_data, generate_timestamped_filename, load_json_data
from tqdm import tqdm

# --- 설정 (Airflow 변수로 관리하거나 상수로 유지 가능) ---
MODEL_NAME = "intfloat/multilingual-e5-large"
BATCH_SIZE = 64  # GPU 사양에 따라 조절 가능
MAIN_TASKS_WEIGHT = 2
QUALIFICATIONS_WEIGHT = 2

# --- 텍스트 생성 함수 ---
def job_to_text(job):
    """임베딩을 위해 공고 데이터를 하나의 텍스트로 결합합니다."""
    main_tasks = (job.get('main_tasks') or '').strip()
    qualifications = (job.get('qualifications') or '').strip()
    preferences = (job.get('preferences') or '').strip()
    tech_stack = (job.get('tech_stack') or '').strip()

    # 특정 섹션에 가중치를 주기 위해 텍스트를 반복
    main_tasks_repeated = (main_tasks + "\n") * MAIN_TASKS_WEIGHT
    qualifications_repeated = (qualifications + "\n") * QUALIFICATIONS_WEIGHT

    return (
        f"직무명: {job.get('job_category', '')}\n"
        f"고용형태: {job.get('employment_type', '')}, 지원대상: {job.get('applicant_type', '')}\n"
        f"주요업무: {main_tasks_repeated}"
        f"자격요건: {qualifications_repeated}"
        f"우대사항: {preferences}\n"
        f"기술스택: {tech_stack}"
    )

# --- Airflow 실행 함수 ---
def embed_jobs_data(ti):
    """
    Airflow Task: 표준화된 JSON 파일을 읽어 임베딩을 생성하고,
    결과를 새로운 JSON 파일로 저장합니다.
    """
    # [수정] 입력 소스를 'tokenize_jobs_task'에서 'standardize_data_task'로 변경
    input_path = ti.xcom_pull(task_ids='standardize_data_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 표준화된 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"임베딩을 위해 데이터 파일을 로드합니다: {input_path}")
    jobs = load_json_data(input_path)
    if not jobs:
        print("임베딩할 데이터가 없습니다.")
        return None

    # 2. 임베딩 모델 로드
    print(f"임베딩 모델을 로드합니다: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)
    
    # 3. 각 공고를 임베딩할 텍스트로 변환
    texts_to_embed = [job_to_text(job) for job in jobs]
    
    # 4. 배치 단위로 임베딩 생성 (라이브러리의 내장 진행률 표시 기능 사용)
    print(f"총 {len(texts_to_embed)}개의 텍스트에 대한 임베딩을 시작합니다 (배치 크기: {BATCH_SIZE})")
    all_embeddings = model.encode(
        texts_to_embed,
        batch_size=BATCH_SIZE,
        convert_to_numpy=True,
        normalize_embeddings=True,  # 정규화 옵션 추가        
        show_progress_bar=True  # 이 옵션이 tqdm 루프를 대체합니다.
    )

    # 5. 원본 데이터에 임베딩 결과 추가
    for i, job in enumerate(jobs):
        job["full_embedding"] = all_embeddings[i].tolist()

    # 6. 결과를 새로운 JSON 파일로 저장
    filename = generate_timestamped_filename("embedded_jobs")
    file_path = save_json_data(jobs, filename)

    print(f"임베딩 완료! 데이터를 새 파일로 저장했습니다: {file_path}")
    
    # 7. 다음 Task(clustering)를 위해 결과 파일 경로를 반환합니다.
    return file_path