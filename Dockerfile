# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.3

# 2. 파이썬 라이브러리 목록 복사 및 설치
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. 루트 권한으로 모든 Playwright 설치 작업 수행
USER root
RUN playwright install-deps && \
    playwright install chrome && \
    mkdir -p /home/airflow/.cache/huggingface && \
    chown -R airflow /home/airflow/.cache

# 4. 최종적으로 airflow 사용자로 전환
USER airflow