# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.3

# 2. 루트 사용자로 전환하여 모든 설치 작업을 수행합니다.
USER root

# 3. requirements.txt 파일을 복사합니다.
COPY requirements.txt .

# 4. 파이썬 라이브러리들을 설치합니다.
RUN pip install --no-cache-dir -r requirements.txt

# 5. Playwright 크롬 브라우저와 시스템 의존성을 한 번에 설치합니다. (가장 효율적인 방법)
RUN playwright install chrome --with-deps

# 6. Hugging Face 캐시 폴더를 생성하고, airflow 사용자에게 소유권을 부여합니다.
RUN mkdir -p /home/airflow/.cache/huggingface && \
    chown -R airflow:airflow /home/airflow/.cache

# 7. 모든 설치가 끝난 후, 컨테이너의 최종 실행 사용자를 airflow로 설정합니다.
USER airflow