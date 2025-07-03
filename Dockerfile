# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.2

# 2. 파이썬 라이브러리 목록 복사
COPY requirements.txt /

# 3. 파이썬 라이브러리 설치 (airflow 사용자 권한)
RUN pip install --no-cache-dir -r /requirements.txt

# --- Playwright 설치 단계 분리 ---

# 4. root 사용자로 전환하여 시스템 의존성만 먼저 설치
USER root
RUN playwright install-deps

# 5. 다시 airflow 사용자로 전환 (매우 중요!)
USER airflow

# 6. 이제 브라우저만 다운로드 (airflow 사용자 권한)
RUN playwright install
