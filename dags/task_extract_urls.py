import asyncio
import json
from urllib.parse import urljoin
from crawl4ai import AsyncWebCrawler
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from utils import save_json_data, generate_timestamped_filename

async def extract_urls_main():
    """원티드 채용 공고 URL을 크롤링하고, 그 결과가 저장된 파일 경로를 반환하는 함수"""
    browser_config = BrowserConfig(verbose=False, user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36")
    extraction_schema = {
        "name": "JobListings",
        "baseSelector": "li.Card_Card__aaatv",
        "fields": [
            {"name": "company_name", "selector": "a[data-attribute-id='position__click']", "type": "attribute", "attribute": "data-company-name"},
            {"name": "job_title", "selector": "a[data-attribute-id='position__click']", "type": "attribute", "attribute": "data-position-name"},
            {"name": "link", "selector": "a[data-attribute-id='position__click']", "type": "attribute", "attribute": "href"}
        ]
    }
    extraction_strategy = JsonCssExtractionStrategy(extraction_schema)
    run_config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        wait_for="css:ul[data-cy='job-list']",
        scan_full_page=True,
        scroll_delay=1,
        cache_mode=CacheMode.BYPASS,
        stream=True
    )

    all_job_listings = []
    url_to_crawl = "https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.latest_order&years=-1&locations=all"
    base_wanted_url = "https://www.wanted.co.kr"

    print(f"원티드에서 채용 공고 크롤링을 시작합니다: {url_to_crawl}")

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(url=url_to_crawl, config=run_config)

        if result.success and result.extracted_content:
            extracted_data = json.loads(result.extracted_content) if isinstance(result.extracted_content, str) else result.extracted_content
            for job in extracted_data:
                if 'link' in job and job['link'].startswith('/'):
                    job['link'] = urljoin(base_wanted_url, job['link'])
            all_job_listings.extend(extracted_data)
        else:
            print(f"\n크롤링 실패: {result.url} - {result.error_message}")
            raise ValueError("URL 목록 추출에 실패했습니다.")

    if all_job_listings:
        output_data = {"Wanted Job Listings": all_job_listings}
        filename = generate_timestamped_filename("wanted_job_listings")
        # 저장 후 파일의 전체 경로를 반환하도록 수정
        file_path = save_json_data(output_data, filename)
        print(f"\n총 {len(all_job_listings)}개의 채용 공고 링크 및 정보 저장 완료: {file_path}")
        return file_path
    else:
        print("\n추출된 채용 공고 정보가 없습니다.")
        return None

def extract_urls():
    """비동기 함수를 동기적으로 실행하기 위한 래퍼 함수"""
    return asyncio.run(extract_urls_main())