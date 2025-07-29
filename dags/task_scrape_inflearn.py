import json
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from utils import save_json_data, generate_timestamped_filename

def scrape_inflearn_courses():
    """
    인프런 웹사이트에서 IT 프로그래밍 강의 목록을 스크래핑하고 JSON 파일로 저장한 뒤,
    결과 파일의 경로를 반환합니다.
    """
    all_courses_data = []
    page_num = 1
    MAX_PAGE = 5 # 테스트를 위해 5페이지로 제한, 필요시 늘릴 수 있음

    print("인프런 스크래핑을 시작합니다...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page()
        
        try:
            while page_num <= MAX_PAGE:
                url = f'https://www.inflearn.com/courses/it-programming?sort=RECENT&page_number={page_num}'
                print(f"\n--- 페이지 {page_num} 스크래핑 중: {url} ---")
                page.goto(url, wait_until='networkidle', timeout=30000)
                
                # 페이지의 모든 강의 카드 요소를 찾음
                course_cards = page.locator("a.course_card_front").all()

                if not course_cards:
                    print(f"페이지 {page_num}에서 강의를 찾을 수 없습니다. 스크래핑을 종료합니다.")
                    break

                print(f"✅ 총 {len(course_cards)}개의 강의를 찾았습니다. 정보 추출 중...")
                for card in course_cards:
                    title = card.locator("div.course_title").text_content(timeout=5000).strip()
                    instructor = card.locator("div.instructor").text_content(timeout=5000).strip()
                    skills_raw = card.locator("div.course_skills > span").all_text_contents()
                    skills = ", ".join(skill.strip() for skill in skills_raw)
                    link = card.get_attribute('href')
                    
                    all_courses_data.append({
                        "title": title,
                        "skills": skills,
                        "instructor": instructor,
                        "link": f"https://www.inflearn.com{link}"
                    })
                page_num += 1

        except PlaywrightTimeoutError:
            print(f"페이지 {page_num} 로딩 시간을 초과했습니다.")
            # 오류가 발생해도 현재까지 수집된 데이터는 저장하도록 함
        except Exception as e:
            print(f"스크래핑 중 알 수 없는 오류가 발생했습니다: {e}")
            raise # Airflow Task를 실패 처리하기 위해 오류를 다시 발생시킴
        finally:
            browser.close()

    if not all_courses_data:
        print("추출된 강의 데이터가 없습니다.")
        return None

    # 수집된 데이터를 파일로 저장하고 경로를 반환
    filename = generate_timestamped_filename("inflearn_courses")
    file_path = save_json_data(all_courses_data, filename)
    print(f"\n크롤링 성공! 총 {len(all_courses_data)}개의 강의 정보를 {file_path} 파일로 저장했습니다.")
    return file_path