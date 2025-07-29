import json
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_sync # Stealth 라이브러리 임포트
from utils import save_json_data, generate_timestamped_filename

def scrape_inflearn_courses():
    """
    인프런 웹사이트에서 봇 감지를 우회하여 IT 프로그래밍 강의 목록을 스크래핑하고
    JSON 파일로 저장한 뒤, 결과 파일의 경로를 반환합니다.
    """
    all_courses_data = []
    page_num = 1
    MAX_PAGE = 54 # 동료분 코드와 동일하게 전체 페이지 설정

    print("인프런 스크래핑을 시작합니다 (Stealth 모드)...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        
        # [핵심] 페이지에 stealth 기능을 적용하여 봇 감지를 우회합니다.
        stealth_sync(page)
        
        try:
            while page_num <= MAX_PAGE:
                url = f'https://www.inflearn.com/courses/it-programming?order=recent&page={page_num}&sort=RECENT'
                print(f"\n--- 페이지 {page_num}/{MAX_PAGE} 스크래핑 중 ---")
                page.goto(url, wait_until='domcontentloaded', timeout=60000)

                # 동료분의 새로운 XPath를 Playwright에 맞게 적용
                courses_xpath = "//h2[text()='강의 리스트']/following::a[starts-with(@href, '/course/') and .//article]"
                
                # 첫 번째 강의 카드가 나타날 때까지 대기
                page.wait_for_selector(f"xpath={courses_xpath}", timeout=30000)
                course_cards = page.locator(f"xpath={courses_xpath}").all()

                if not course_cards:
                    print(f"페이지 {page_num}에서 강의를 찾을 수 없습니다. 다음 페이지로 넘어갑니다.")
                    page_num += 1
                    continue

                print(f"✅ 총 {len(course_cards)}개의 강의를 찾았습니다. 정보 추출 중...")
                for card in course_cards:
                    try:
                        link = card.get_attribute('href')
                        
                        article = card.locator("article").first
                        info_container = article.locator("xpath=./div[2]/div[1]")
                        title = info_container.locator("xpath=./p[1]").text_content().strip()
                        instructor = info_container.locator("xpath=./p[2]").text_content().strip()
                        
                        skills = "관련 기술 정보 없음"
                        try:
                            skills_full_text = article.locator("xpath=.//p[span]").text_content()
                            if '/' in skills_full_text:
                                skills = skills_full_text.split('/', 1)[1].strip()
                        except Exception:
                            pass
                        
                        price = "가격 정보 없음"
                        try:
                            price_element = article.locator("xpath=(.//p[starts-with(text(), '₩') or text()='무료'])[last()]")
                            price = price_element.text_content().strip()
                        except Exception:
                            pass

                        all_courses_data.append({
                            "title": title,
                            "skills": skills,
                            "instructor": instructor,
                            "price": price,
                            "link": f"https://www.inflearn.com{link}"
                        })
                    except Exception as e:
                        print(f"⚠️ 개별 카드에서 정보 추출 중 오류 발생 (건너뜁니다): {e}")
                
                page_num += 1

        except Exception as e:
            print(f"🚨 스크래핑 중 오류 발생: {e}")
            raise
        finally:
            context.close()
            browser.close()

    if not all_courses_data:
        print("추출된 강의 데이터가 없습니다.")
        return None

    filename = generate_timestamped_filename("inflearn_courses")
    file_path = save_json_data(all_courses_data, filename)
    print(f"\n크롤링 성공! 총 {len(all_courses_data)}개의 강의 정보를 {file_path} 파일로 저장했습니다.")
    return file_path