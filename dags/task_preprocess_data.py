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
    """LM Studio 로컬 서버에 연결하는 LLM 클라이언트를 생성합니다."""
    client = OpenAI(
        base_url="http://host.docker.internal:1234/v1",
        api_key="lm-studio", # 실제 키는 필요 없으나, 형식상 아무 문자열이나 입력
    )
    return client


def extract_tech_stack_with_llm(client, job_post_text: str) -> str | None:
    """LLM을 사용하여 주어진 텍스트에서 기술 스택을 추출하고 표준화합니다."""
    if not job_post_text:
        return None
        
    # LLM에게 보낼 프롬프트(명령어)
    prompt = f"""
    You are an expert in analyzing job descriptions to accurately extract and standardize technology stacks.
    From the provided text of a job description, please find all mentioned technology stacks, such as programming languages, frameworks, libraries, databases, cloud services, and development tools.

    Instructions:
    Convert each technology stack to its most widely recognized standard English name (e.g., '파이썬' -> 'Python', '리액트' -> 'React').
    Sort the extracted technology stacks in alphabetical order.
    The result must be returned only as a comma-separated string (e.g., "AWS, Docker, MySQL, Python, React").
    Never guess or add any technologies that are not mentioned in the text.
    Do not include any descriptions or sentences other than the technology stacks.
    If no technology stacks are mentioned in the text, return null.

    ---
    {job_post_text}
    """

    try:
        chat_completion = client.chat.completions.create(
            model="mistralai/mistral-nemo-instruct-2407",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
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