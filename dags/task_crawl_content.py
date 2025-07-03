import asyncio
import json
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
from utils import save_json_data, generate_timestamped_filename, load_json_data

def load_urls_from_json(file_path: str) -> list:
    """지정된 JSON 파일에서 링크 목록을 로드합니다."""
    try:
        data = load_json_data(file_path)
        job_listings = data.get("Wanted Job Listings", [])
        return [job.get("link") for job in job_listings if job.get("link")]
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"오류: 지정된 JSON 파일을 찾을 수 없거나 파싱할 수 없습니다: {file_path}")
        return []

def parse_job_details(html_content: str) -> dict | None:
    """HTML에서 __NEXT_DATA__ JSON과 기술 스택을 추출하고, 원하는 테이블 구조로 파싱합니다."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not script_tag: return None

        json_data = json.loads(script_tag.string)
        job_data = json_data['props']['pageProps']['initialData']
        company_data = job_data.get('company', {})

        career = job_data.get('career', {})
        applicant_type = "무관"
        if career.get('is_newbie') and career.get('is_expert'): applicant_type = f"신입/경력({career.get('annual_from', '0')}~{career.get('annual_to','-')}년)"
        elif career.get('is_newbie'): applicant_type = "신입"
        elif career.get('is_expert'): applicant_type = f"경력({career.get('annual_from', '0')}~{career.get('annual_to','-')}년)"

        def clean_text(text):
            return ' '.join(text.splitlines()) if text else None

        structured_data = {
            'id': job_data.get('id'), 'company_id': company_data.get('company_id'), 'job_position': job_data.get('position'),
            'employment_type': job_data.get('employment_type'), 'applicant_type': applicant_type, 'posting_date': job_data.get('confirm_time'),
            'main_tasks': clean_text(job_data.get('main_tasks')), 'qualifications': clean_text(job_data.get('requirements')),
            'preferences': clean_text(job_data.get('preferred_points')), 'tech_stack': None
        }
        
        tech_stack_section = soup.select_one("article.JobSkillTags_JobSkillTags__Oy6Uh")
        if tech_stack_section:
            tech_stack_list = [tag.get_text(strip=True) for tag in tech_stack_section.select("ul > li.SkillTagItem_SkillTagItem__MAo9X > span")]
            if tech_stack_list: structured_data['tech_stack'] = ', '.join(tech_stack_list)
        
        return structured_data
    except Exception as e:
        # print(f"\n오류: 파싱 중 오류 발생: {e}") # 로그가 너무 많아질 수 있으므로 주석 처리
        return None

async def crawl_content_main(input_json_path: str):
    """URL 목록이 담긴 JSON 파일을 입력받아, 상세 페이지를 크롤링하고 최종 결과를 저장합니다."""
    if not input_json_path:
        print("입력 JSON 파일 경로가 없습니다. 이전 작업이 실패했을 수 있습니다.")
        return

    job_detail_urls = load_urls_from_json(input_json_path)
    if not job_detail_urls:
        print("크롤링할 URL이 없습니다.")
        return

    print(f"JSON 파일({input_json_path})에서 총 {len(job_detail_urls)}개의 URL을 로드했습니다.")

    browser_config = BrowserConfig(verbose=False, user_agent_mode="random")
    dispatcher = MemoryAdaptiveDispatcher(max_session_permit=3)
    run_config = CrawlerRunConfig(cache_mode=CacheMode.ENABLED, stream=True)
    all_structured_data = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results_iterator = await crawler.arun_many(urls=job_detail_urls, config=run_config, dispatcher=dispatcher)
        async for result in results_iterator:
            if result.success and result.html:
                structured_job_data = parse_job_details(result.html)
                if structured_job_data:
                    all_structured_data.append(structured_job_data)

    if all_structured_data:
        filename = generate_timestamped_filename("wanted_structured_jobs")
        file_path = save_json_data(all_structured_data, filename)
        print(f"\n총 {len(all_structured_data)}개의 구조화된 채용 공고 저장 완료: {file_path}")
    else:
        print("\n추출된 채용 공고 정보가 없습니다.")

def crawl_content(ti):
    """Airflow Task Instance(ti)에서 XCom을 통해 파일 경로를 받아 비동기 함수를 실행하는 래퍼 함수"""
    input_path = ti.xcom_pull(task_ids='extract_urls_task', key='return_value')
    if input_path:
        asyncio.run(crawl_content_main(input_path))
    else:
        print("XCom으로부터 파일 경로를 가져오지 못했습니다.")