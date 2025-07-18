# 1. Airflow 공식 이미지를 기반으로 시작합니다.
FROM apache/airflow:3.0.3

# 2. 루트(root) 사용자로 전환하여 모든 시스템 레벨의 작업을 먼저 수행합니다.
USER root

# 3. Playwright가 필요로 하는 모든 시스템 라이브러리를 미리 설치합니다.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libdbus-glib-1-2 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 \
    && apt-get clean

# 4. 나중에 airflow 사용자가 사용할 캐시 폴더들을 미리 만들고, 소유권을 부여합니다.
#    'airflow' 사용자/그룹 이름 대신 UID/GID를 직접 사용하여 권한 문제를 근본적으로 해결합니다.
RUN mkdir -p /home/airflow/.cache/pip /home/airflow/.cache/huggingface /home/airflow/.cache/ms-playwright && \
    chown -R 50000:0 /home/airflow/.cache

# 5. 이제 airflow 사용자로 전환하여, 사용자 레벨의 작업을 안전하게 수행합니다.
USER airflow

# 6. requirements.txt 복사 및 파이썬 라이브러리 설치
COPY --chown=airflow:0 requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 7. Playwright 브라우저 '실행 파일'만 설치 (시스템 의존성 설치 없이)
RUN playwright install chrome

# 8. Hugging Face 모델 미리 다운로드하여 이미지에 포함
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-large')"