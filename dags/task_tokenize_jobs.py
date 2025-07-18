import asyncio
import json
import re
import aiohttp
import certifi
import ssl
from airflow.hooks.base import BaseHook
from utils import save_json_data, generate_timestamped_filename, load_json_data
from tqdm.asyncio import tqdm

# --- 설정 ---
MODEL_NAME = "deepseek/deepseek-r1-0528-qwen3-8b"
MAX_CONCURRENT_REQUESTS = 10  # 동시 요청 수 제어
MAX_RETRIES = 3
RETRY_DELAY = 2

# --- 2차 전처리 함수 ---
def clean_and_standardize_keyword(keyword):
    """개별 키워드를 표준화하고 정리합니다."""
    if not isinstance(keyword, str):
        return ""
    cleaned = keyword.lower().strip().replace(' ', '-')
    cleaned = re.sub(r'[^a-z0-9가-힣-]', '', cleaned)
    return cleaned

# --- LLM 호출 함수 ---
async def tokenize_with_llm(session, semaphore, text, section_name, job_id):
    """비동기적으로 LLM을 호출하여 키워드를 추출합니다."""
    async with semaphore:
        if not isinstance(text, str) or not text.strip():
            return []

        prompt = f"""
            # 지시사항
            당신은 채용 공고를 분석하여 핵심 키워드를 JSON 형식으로 추출하는 전문가입니다.
            1.  핵심 키워드를 '명사' 위주로 가장 짧게 추출합니다.
            2.  '~경험', '~우대' 등의 불필요한 단어는 제거합니다.
            3.  결과는 "keywords" 라는 키를 가진 JSON 객체 안에 배열(array)로 담아주세요.
            
            # 예시
            - 입력 텍스트: "• C/C++을 활용한 프로젝트 개발 경험이 있으신 분"
            - 출력 결과: {{"keywords": ["C/C++", "프로젝트"]}}

            # 분석할 텍스트 ({section_name})
            ---
            {text}
            ---
        """
        
        connection = BaseHook.get_connection('openrouter_default')
        api_key = connection.password
        base_url = connection.host
        
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": MODEL_NAME,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"}
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(f"{base_url}/chat/completions", headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=180)) as response:
                    response.raise_for_status()
                    result = await response.json()
                    json_content = json.loads(result['choices'][0]['message']['content'])
                    return json_content.get("keywords", [])
            except Exception as e:
                if attempt + 1 == MAX_RETRIES:
                    print(f"\n🚨 [ID:{job_id}] 모든 재시도 실패. 최종 오류: {e}")
                    return ["최종 API 오류 발생"]
                await asyncio.sleep(RETRY_DELAY)

# --- 메인 비동기 처리 함수 ---
async def process_jobs_main(input_path):
    try:
        data = load_json_data(input_path)
        if not data:
            print("처리할 데이터가 없습니다.")
            return None
    except Exception as e:
        print(f"🚨 오류: '{input_path}' 파일 로드 실패. 오류: {e}")
        return None

    print(f"✅ 총 {len(data)}개의 공고에 대한 토큰화 및 후처리 작업을 시작합니다.")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        tasks = []
        for job in data:
            job_id = job.get('id', 'N/A')
            
            # 각 공고에 대해 3개의 필드를 동시에 LLM에 요청하는 작업을 생성
            llm_tasks = [
                tokenize_with_llm(session, semaphore, job.get('main_tasks', ''), "주요 업무", job_id),
                tokenize_with_llm(session, semaphore, job.get('qualifications', ''), "자격 요건", job_id),
                tokenize_with_llm(session, semaphore, job.get('preferences', ''), "우대 사항", job_id)
            ]
            tasks.append(asyncio.gather(*llm_tasks))

        # tqdm으로 전체 공고 처리 진행률 표시
        all_results = await tqdm.gather(*tasks, desc="LLM 키워드 추출 중")

    processed_data = []
    for i, job in enumerate(data):
        main_tasks_skills, required_skills, preferred_skills = all_results[i]
        
        cleaned_job = {'id': job.get('id')}
        
        # 후처리 로직 적용
        for key, skills_list in zip(['main_tasks_skills', 'required_skills', 'preferred_skills'], [main_tasks_skills, required_skills, preferred_skills]):
            seen_keywords = set()
            unique_cleaned_list = []
            for keyword in skills_list:
                cleaned_keyword = clean_and_standardize_keyword(keyword)
                if cleaned_keyword and cleaned_keyword not in seen_keywords:
                    unique_cleaned_list.append(cleaned_keyword)
                    seen_keywords.add(cleaned_keyword)
            cleaned_job[key] = unique_cleaned_list
            
        processed_data.append(cleaned_job)

    # 최종 결과 파일 저장
    filename = generate_timestamped_filename("tokenized_jobs")
    file_path = save_json_data(processed_data, filename)
    print(f"\n🎉 성공! 모든 작업이 완료되었습니다. 결과가 '{file_path}' 파일에 저장되었습니다.")
    return file_path

# --- Airflow 실행 함수 ---
def tokenize_and_post_process_jobs(ti):
    """Airflow가 호출하는 메인 래퍼 함수"""
    input_path = ti.xcom_pull(task_ids='crawl_content_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 원본 데이터 파일 경로를 가져오지 못했습니다.")
    
    return asyncio.run(process_jobs_main(input_path))