import json
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
from utils import save_json_data, generate_timestamped_filename

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
        
        company = th_element.select_one("p.text-grey-600").text.strip() if th_element.select_one("p.text-grey-600") else None
        course_name = th_element.select_one("p.break-keep").text.strip() if th_element.select_one("p.break-keep") else None
        program_course = td_elements[0].select_one("ul > li > div").text.strip() if len(td_elements) > 0 and td_elements[0].select_one("ul > li > div") else None

        status, deadline = None, None
        if len(td_elements) > 1 and td_elements[1].select_one("div"):
            status_container = td_elements[1].select_one("div")
            status_el = status_container.find('div', class_=lambda c: c and 'whitespace-nowrap' in c and not 'whitespace-pre-wrap' in c if c else False)
            deadline_el = status_container.find('div', class_='whitespace-pre-wrap')
            status = status_el.text.strip() if status_el else None
            deadline = deadline_el.text.strip() if deadline_el else None
        
        cost = None
        if len(td_elements) > 2 and td_elements[2].find('div'):
            cost_text_node = td_elements[2].find('div').find(string=True, recursive=False)
            cost = cost_text_node.strip() if cost_text_node and cost_text_node.strip() else None

        location = None
        if len(td_elements) > 3 and td_elements[3].find('div'):
            location_parts = [part.text.strip() for part in td_elements[3].find_all(['div', 'p'], recursive=False) if part.text.strip()]
            location = f"{location_parts[0]} ({location_parts[1]})" if len(location_parts) > 1 else (location_parts[0] if location_parts else None)

        period = None
        if len(td_elements) > 4 and td_elements[4].find('div'):
            period_parts = [part.text.strip() for part in td_elements[4].find_all(['span', 'div'])]
            if len(period_parts) > 2:
                 period = f"{period_parts[0]} ~ {period_parts[2]} ({period_parts[1]})"
            elif period_parts:
                 period = " ~ ".join(filter(None, period_parts))

        participation_time = None
        if len(td_elements) > 5 and td_elements[5].find('div'):
            time_type_el = td_elements[5].select_one("div > div")
            time_details_el = td_elements[5].select_one("div > ul")
            if time_type_el and time_details_el:
                time_type = time_type_el.text.strip()
                time_details = ' '.join(time_details_el.text.split())
                participation_time = f"{time_type} ({time_details})"

        selection_keywords, tech_stacks = None, None
        if len(td_elements) > 6:
            selection_el = td_elements[6].select_one("div > div")
            selection_keywords = selection_el.text.strip() if selection_el else None
            tech_stack_items = td_elements[6].select("div > ul > li > div")
            tech_stacks = ", ".join([item.text.strip() for item in tech_stack_items]) if tech_stack_items else None

        hiring_info = None
        if len(td_elements) > 7:
             hiring_items = td_elements[7].select("button > ul > li")
             hiring_info = ", ".join([item.text.strip().replace(',', '') for item in hiring_items]) if hiring_items else None

        bootcamp_info = {
            "company": company, "course_name": course_name, "program_course": program_course,
            "status": status, "deadline": deadline, "cost": cost, "location": location,
            "period": period, "participation_time": participation_time, "selection_keywords": selection_keywords,
            "tech_stack": tech_stacks, "hiring_info": hiring_info,
        }
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
            
            # '리스트 보기' 버튼 클릭
            page.evaluate("document.querySelector(\"button:has(svg > path[d^='M1.66675 2.91671'])\").click();")
            
            # 테이블 데이터가 로드될 때까지 대기
            page.wait_for_selector("table > tbody > tr:nth-child(10)", timeout=15000)
            
            html_source = page.content()
            bootcamp_data = extract_bootcamp_data(html_source)
            
            if not bootcamp_data:
                print("추출된 부트캠프 데이터가 없습니다.")
                return None

            filename = generate_timestamped_filename("boottent_bootcamps")
            file_path = save_json_data(bootcamp_data, filename)

            print(f"크롤링 성공! 총 {len(bootcamp_data)}개의 부트캠프 정보를 {file_path} 파일로 저장했습니다.")
            return file_path # 다음 Task를 위해 파일 경로를 반환합니다.
            
        except PlaywrightTimeoutError:
             print(f"오류: 페이지 로딩 또는 데이터 테이블을 찾는 데 시간이 너무 오래 걸립니다.")
             page.screenshot(path="error_screenshot.png")
             print("오류 화면을 error_screenshot.png 파일로 저장했습니다.")
             raise # Airflow에서 Task를 실패 처리하도록 오류를 다시 발생시킴
        except Exception as e:
            print(f"알 수 없는 오류가 발생했습니다: {e}")
            page.screenshot(path="error_screenshot.png")
            print("오류 화면을 error_screenshot.png 파일로 저장했습니다.")
            raise
        finally:
            browser.close()