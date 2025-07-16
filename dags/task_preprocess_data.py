import asyncio
import json
from openai import AsyncOpenAI # 비동기 클라이언트를 임포트합니다.
from tqdm.asyncio import tqdm   # tqdm의 비동기 지원 기능을 임포트합니다.
from utils import save_json_data, generate_timestamped_filename, load_json_data

def load_json_from_path(file_path: str) -> list | None:
    """지정된 경로의 JSON 파일을 읽어 리스트 형태로 반환합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, TypeError) as e:
        print(f"오류: JSON 파일을 읽거나 파싱하는 데 실패했습니다: {e}")
        return None

def get_llm_async_client():
    """LM Studio 로컬 서버에 연결하는 비동기 LLM 클라이언트를 생성합니다."""
    return AsyncOpenAI(
        base_url="http://host.docker.internal:1234/v1",
        api_key="lm-studio",
    )

async def extract_tech_stack_with_llm_async(semaphore, client, job_post_text: str, index: int) -> tuple[int, str | None]:
    """Semaphore를 사용하여 동시 실행을 제어하며 LLM을 호출합니다."""
    # async with 구문으로 semaphore를 획득합니다. 작업이 끝나면 자동으로 반환됩니다.
    async with semaphore:
        if not job_post_text:
            return index, None
            
        prompt = f"""
        You are an expert in analyzing job descriptions to accurately extract and standardize technology stacks.
        From the provided text of a job description, please find all mentioned technology stacks, such as programming languages, frameworks, libraries, databases, cloud services, and development tools.

        Instructions:
        Convert each technology stack to its most widely recognized standard English name (e.g., '파이썬' -> 'Python', '리액트' -> 'React').
        Sort the extracted technology stacks in alphabetical order.
        The result must be returned only as a comma-separated string (e.g., "AWS, Docker, MySQL, Python, React").
        Never guess or add any technologies that are not mentioned in the text.
        Do not include any descriptions or sentences other than the technology stacks.
        If no technology stacks are mentioned in the text, **return an empty string.**

        ---
        {job_post_text}
        """

        try:
            # 비동기 함수 호출을 위해 'await' 키워드를 사용합니다.
            chat_completion = await client.chat.completions.create(
                model="mistralai/mistral-nemo-instruct-2407",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=256,
            )
            tech_stack_str = chat_completion.choices[0].message.content.strip()
            
            # 결과가 빈 문자열이면 None으로 처리하여 일관성을 유지합니다.
            if not tech_stack_str or tech_stack_str.lower() in ["null", "none"]:
                return index, None
                
            return index, tech_stack_str.replace('"', '')
        except Exception as e:
            # 오류가 발생해도 전체 작업이 멈추지 않도록 None을 반환하고 로그를 남깁니다.
            print(f"LLM API 호출 중 오류 발생 (공고 인덱스 {index}): {e}")
            return index, None

async def preprocess_data_main(json_file_path: str):
    """
    JSON 파일을 읽어, Semaphore로 제어되는 병렬 API 호출을 실행하고,
    결과를 취합하여 새로운 JSON 파일로 저장합니다.
    """
    raw_data = load_json_from_path(json_file_path)
    if not raw_data:
        print("전처리할 데이터가 없습니다.")
        return None

    # 동시 실행 개수를 10개로 제한하는 Semaphore 생성 (PC 사양에 따라 조절 가능)
    semaphore = asyncio.Semaphore(10)
    llm_client = get_llm_async_client()
    tasks = []

    print(f"총 {len(raw_data)}개의 데이터에 대한 비동기 API 호출을 준비합니다 (동시 실행: 10개)")
    for i, job in enumerate(raw_data):
        text_for_extraction = " ".join(filter(None, [
            job.get("main_tasks"),
            job.get("qualifications"),
            job.get("preferences")
        ]))
        # 각 API 호출을 '작업(Task)'으로 만들어 리스트에 추가합니다.
        tasks.append(extract_tech_stack_with_llm_async(semaphore, llm_client, text_for_extraction, i))

    # tqdm.gather를 사용하여 진행률을 표시하며 모든 작업을 동시에 실행합니다.
    results = await tqdm.gather(*tasks, desc="LLM 기술 스택 추출 중")
    
    print(f"모든 LLM API 호출이 완료되었습니다. (총 {len(results)}개)")

    # 결과의 순서가 섞일 수 있으므로, 원래 인덱스를 기준으로 정렬합니다.
    results.sort(key=lambda x: x[0])
    
    preprocessed_data = []
    for i, job in enumerate(raw_data):
        _original_index, extracted_stack = results[i]
        job["tech_stack"] = extracted_stack
        preprocessed_data.append(job)

    filename = generate_timestamped_filename("preprocessed_jobs")
    file_path = save_json_data(preprocessed_data, filename)
    print(f"전처리 완료된 데이터를 새 파일로 저장했습니다: {file_path}")
    return file_path

def preprocess_data(ti):
    """Airflow가 호출하는 메인 함수. 비동기 함수를 실행시키는 역할을 합니다."""
    json_file_path = ti.xcom_pull(task_ids='crawl_content_task', key='return_value')
    if not json_file_path:
        raise ValueError("XCom으로부터 원본 데이터 파일 경로를 가져오지 못했습니다.")

    # asyncio.run을 통해 비동기 메인 함수를 실행하고 그 결과를 반환합니다.
    return asyncio.run(preprocess_data_main(json_file_path))