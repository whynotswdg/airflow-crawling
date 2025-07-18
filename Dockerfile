# 1. Airflow 공식 이미지를 기반으로 시작합니다.
FROM apache/airflow:3.0.3

# 2. 루트(root) 사용자로 전환하여 시스템 레벨의 작업을 먼저 수행합니다.
USER root

# 3. Playwright가 필요로 하는 시스템 라이브러리들을 미리 설치합니다.
#    이 작업은 반드시 root 권한으로 실행해야 합니다.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libdbus-glib-1-2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && apt-get clean

# 4. 이제 airflow 사용자로 전환하여, 사용자 레벨의 작업을 수행합니다.
USER airflow

# 5. requirements.txt 복사 및 파이썬 라이브러리 설치
#    이 단계에서 모든 분석용 라이브러리가 설치됩니다.
COPY --chown=airflow:airflow requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Playwright 브라우저 '실행 파일'만 설치 (시스템 의존성 설치 없이)
#    시스템 라이브러리는 이미 root로 설치했으므로, 여기서는 --with-deps 옵션을 사용하지 않습니다.
RUN playwright install chrome

# 7. Hugging Face 모델 미리 다운로드하여 이미지에 포함
#    airflow 사용자로 실행하여, /home/airflow/.cache 폴더에 정상적으로 저장되도록 합니다.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-large')"