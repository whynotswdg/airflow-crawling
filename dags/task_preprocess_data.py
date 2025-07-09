import json
from airflow.hooks.base import BaseHook
from openai import OpenAI

def load_json_from_path(file_path: str) -> list | None:
    """지정된 경로의 JSON 파일을 읽어 리스트 형태로 반환합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
        print(f"오류: JSON 파일을 읽거나 파싱하는 데 실패했습니다: {e}")
        return None

def get_llm_client():
    """Airflow Connection에서 OpenRouter API 키를 가져와 LLM 클라이언트를 생성합니다."""
    # 1. 'openrouter_default' Connection 정보를 가져옵니다.
    connection = BaseHook.get_connection('openrouter_default')
    
    # 2. 클라이언트를 초기화합니다.
    # base_url은 Connection의 Host 필드를, api_key는 Password 필드를 사용합니다.
    client = OpenAI(
        base_url=connection.host,
        api_key=connection.password,
    )
    return client

def extract_tech_stack_with_llm(client, job_post_text: str) -> str | None:
    """LLM을 사용하여 주어진 텍스트에서 기술 스택을 추출하고 표준화합니다."""
    if not job_post_text:
        return None
        
    # LLM에게 보낼 프롬프트(명령어)
    prompt = f"""
    당신은 채용 공고를 분석하여 기술 스택을 정확하게 추출하고 표준화하는 전문가입니다.
    아래 "---"로 구분된 채용 공고 본문 내용에서 언급된 모든 프로그래밍 언어, 프레임워크, 라이브러리, 데이터베이스, 클라우드 서비스, 개발 도구 등의 기술 스택을 찾아주세요.

    # 지침:
    - 각 기술 스택은 가장 널리 알려진 영문 표준 이름으로 변환해주세요. (예: '파이썬' -> 'Python', '리액트' -> 'React')
    - 결과는 반드시 쉼표(,)로 구분된 문자열 형태로만 반환해주세요. (예: "Python, React, AWS, Docker, MySQL")
    - 본문에 언급되지 않은 기술은 절대 추측해서 추가하지 마세요.
    - 기술 스택 외에 다른 설명이나 문장은 절대 포함하지 마세요.

    ---
    {job_post_text}
    """

    try:
        chat_completion = client.chat.completions.create(
            model="meta-llama/llama-4-maverick", # LLM 
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0, # 일관된 결과를 위해 0으로 설정
            max_tokens=256,
        )
        tech_stack_str = chat_completion.choices[0].message.content.strip()
        # LLM이 간혹 따옴표를 붙여서 반환하는 경우가 있어 제거
        return tech_stack_str.replace('"', '')
    except Exception as e:
        print(f"LLM API 호출 중 오류 발생: {e}")
        return None


def preprocess_data(ti):
    """
    이전 Task의 결과인 JSON 파일을 읽어, 각 공고의 기술 스택을 LLM으로 추출하고,
    결과를 새로운 JSON 파일로 저장합니다.
    """
    from utils import save_json_data, generate_timestamped_filename

    # 이전 Task에서 전달받은 파일 경로를 가져옵니다.
    json_file_path = ti.xcom_pull(task_ids='save_to_db_task', key='return_value')
    if not json_file_path:
        # save_to_db_task가 return값이 없으므로 crawl_content_task에서 가져옵니다.
        json_file_path = ti.xcom_pull(task_ids='crawl_content_task', key='return_value')

    if not json_file_path:
        raise ValueError("XCom으로부터 원본 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"전처리할 원본 데이터 파일을 로드합니다: {json_file_path}")
    raw_data = load_json_from_path(json_file_path)
    if not raw_data:
        print("전처리할 데이터가 없습니다.")
        return

    llm_client = get_llm_client()
    preprocessed_data = []

    print(f"총 {len(raw_data)}개의 데이터 전처리를 시작합니다...")
    for i, job in enumerate(raw_data):
        # 1. 기술 스택을 추출할 텍스트 필드들을 하나로 합칩니다.
        text_for_extraction = " ".join(
            filter(None, [
                job.get("main_tasks"),
                job.get("qualifications"),
                job.get("preferences")
            ])
        )

        # 2. LLM을 호출하여 기술 스택을 추출합니다.
        extracted_stack = extract_tech_stack_with_llm(llm_client, text_for_extraction)
        
        # 3. 기존 'tech_stack' 필드의 값을 새로 추출한 값으로 덮어씁니다.
        job["tech_stack"] = extracted_stack
        preprocessed_data.append(job)
        
        # 50개마다 진행 상황 출력
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(raw_data)}개 처리 완료")

    print("데이터 전처리 완료.")

    # 4. 전처리된 최종 데이터를 새로운 JSON 파일로 저장합니다.
    filename = generate_timestamped_filename("preprocessed_jobs")
    file_path = save_json_data(preprocessed_data, filename)

    print(f"전처리 완료된 데이터를 새 파일로 저장했습니다: {file_path}")
    
    # 다음 Task를 위해 새로운 파일 경로를 반환합니다. (지금은 마지막 단계)
    return file_path