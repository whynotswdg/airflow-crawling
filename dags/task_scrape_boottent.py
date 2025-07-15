import json
import re
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from utils import generate_timestamped_filename, save_json_data

# -----------------------------------------------------------------------------
# 헬퍼 함수
# -----------------------------------------------------------------------------
def format_date(date_str: str) -> str | None:
    """ 'YY.MM.DD' 형식의 문자열을 'YYYY-MM-DD'로 변환합니다. """
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), '%y.%m.%d')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None

def parse_period_data(period_cell):
    """
    기간(period) 정보를 담고 있는 table cell(<td>)에서
    시작일, 종료일, 기간(개월)을 추출합니다.
    """
    if not period_cell:
        return None, None, None, None

    container = period_cell.find('div')
    if not container:
        return None, None, None, None

    spans = container.find_all('span', recursive=False)
    div_duration = container.find('div', recursive=False)

    start_date_str = spans[0].text.strip() if len(spans) > 0 else None
    end_date_str = spans[1].text.strip() if len(spans) > 1 else None
    duration_str = div_duration.text.strip() if div_duration else None

    duration_months = None
    if duration_str:
        match = re.search(r'[\d.]+', duration_str)
        if match:
            duration_months = float(match.group())

    start_date = format_date(start_date_str)
    end_date, end_date_text = None, None
    if end_date_str:
        formatted_end_date = format_date(end_date_str)
        if formatted_end_date:
            end_date = formatted_end_date
        else:
            end_date_text = end_date_str

    return start_date, end_date, end_date_text, duration_months

# -----------------------------------------------------------------------------
# 메인 스크래핑 로직
# -----------------------------------------------------------------------------
def extract_bootcamp_data(html_content):
    """HTML 내용에서 부트캠프 데이터를 '리스트 보기' 기준으로 추출하여 JSON 형태로 반환합니다."""
    soup = BeautifulSoup(html_content, 'html.parser')
    bootcamp_list = []
    bootcamp_rows = soup.select("table > tbody > tr")

    for row in bootcamp_rows:
        th_element = row.find('th')
        td_elements = row.find_all('td')

        if not th_element or len(td_elements) < 8:
            continue
        
        deadline_el = td_elements[1].select_one("div.whitespace-pre-wrap") if len(td_elements) > 1 else None
        deadline_str = deadline_el.text.strip() if deadline_el else None
        deadline = format_date(deadline_str)

        period_cell = td_elements[4] if len(td_elements) > 4 else None
        start_date, end_date, end_date_text, duration_months = parse_period_data(period_cell)

        tech_stack_items = td_elements[6].select("div > ul > li > div") if len(td_elements) > 6 else []
        tech_stacks = [item.text.strip() for item in tech_stack_items]

        hiring_items = td_elements[7].select("button > ul > li") if len(td_elements) > 7 else []
        hiring_info = [item.text.strip().replace(',', '') for item in hiring_items]
        
        company = th_element.select_one("p.text-grey-600").text.strip() if th_element.select_one("p.text-grey-600") else None
        course_name = th_element.select_one("p.break-keep").text.strip() if th_element.select_one("p.break-keep") else None
        program_course = td_elements[0].select_one("ul > li > div").text.strip() if len(td_elements) > 0 and td_elements[0].select_one("ul > li > div") else None
        status_el = td_elements[1].select_one("div > div:not(.whitespace-pre-wrap)") if len(td_elements) > 1 else None
        status = status_el.text.strip() if status_el else None
        cost_text_node = td_elements[2].find('div').find(string=True, recursive=False) if len(td_elements) > 2 and td_elements[2].find('div') else None
        cost = cost_text_node.strip() if cost_text_node and cost_text_node.strip() else None
        location_parts = [part.text.strip() for part in td_elements[3].find_all(['div', 'p'], recursive=False) if part.text.strip()] if len(td_elements) > 3 and td_elements[3].find('div') else []
        location = f"{location_parts[0]} ({location_parts[1]})" if len(location_parts) > 1 else (location_parts[0] if location_parts else None)
        time_type_el = td_elements[5].select_one("div > div") if len(td_elements) > 5 else None
        time_details_el = td_elements[5].select_one("div > ul") if len(td_elements) > 5 else None
        participation_time = f"{time_type_el.text.strip()} ({' '.join(time_details_el.text.split())})" if time_type_el and time_details_el else None
        selection_el = td_elements[6].select_one("div > div") if len(td_elements) > 6 else None
        selection_keywords = selection_el.text.strip() if selection_el else None
        
        bootcamp_info = {
            "company": company, "course_name": course_name, "program_course": program_course,
            "status": status, "deadline": deadline, "cost": cost, "location": location,
            "start_date": start_date, "end_date": end_date, "end_date_text": end_date_text,
            "duration_months": duration_months, "participation_time": participation_time, 
            "selection_keywords": selection_keywords, "tech_stack": tech_stacks, "hiring_info": hiring_info,
        }
        
        if not bootcamp_info.get("end_date_text"):
            bootcamp_info.pop("end_date_text", None)
            
        bootcamp_list.append(bootcamp_info)
        
    return bootcamp_list

def scrape_boottent_data():
    """
    부트텐트 웹사이트에서 부트캠프 정보를 스크래핑하고 JSON 파일로 저장한 뒤,
    결과 파일의 경로를 반환합니다.
    """
    URL = "https://boottent.com/camps"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"부트텐트 스크래핑을 시작합니다: {URL}")
            page.goto(URL, wait_until='networkidle', timeout=30000)
            
            page.evaluate("document.querySelector(\"button:has(svg > path[d^='M1.66675 2.91671'])\").click();")
            
            page.wait_for_selector("table > tbody > tr:nth-child(10)", timeout=15000)
            
            html_source = page.content()
            bootcamp_data = extract_bootcamp_data(html_source)
            
            if not bootcamp_data:
                print("추출된 부트캠프 데이터가 없습니다.")
                return None

            # utils.py의 함수들을 사용하여 파일 이름 생성 및 저장
            filename = generate_timestamped_filename("boottent_bootcamps")
            file_path = save_json_data(bootcamp_data, filename)

            print(f"크롤링 성공! 총 {len(bootcamp_data)}개의 부트캠프 정보를 {file_path} 파일로 저장했습니다.")
            return file_path
            
        except PlaywrightTimeoutError:
             print(f"오류: 페이지 로딩 또는 데이터 테이블을 찾는 데 시간이 너무 오래 걸립니다.")
             page.screenshot(path="error_screenshot.png")
             print("오류 화면을 error_screenshot.png 파일로 저장했습니다.")
             raise
        except Exception as e:
            print(f"알 수 없는 오류가 발생했습니다: {e}")
            page.screenshot(path="error_screenshot.png")
            print("오류 화면을 error_screenshot.png 파일로 저장했습니다.")
            raise
        finally:
            browser.close()

# -----------------------------------------------------------------------------
# 스크립트 실행
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    scraped_file_path = scrape_boottent_data()
    if scraped_file_path:
        print(f"\n작업 완료. 결과 파일: {scraped_file_path}")