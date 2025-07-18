# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.3

# 2. 루트 사용자로 전환하여 모든 시스템 레벨의 작업을 먼저 수행
USER root

# a. Playwright가 필요로 하는 모든 시스템 의존성 라이브러리를 미리 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libdbus-glib-1-2 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 \
    && apt-get clean

# b. Hugging Face 캐시 폴더 생성 및 airflow 사용자에게 소유권 부여
#    'airflow:airflow' 대신, Airflow의 공식 UID(50000)와 GID(0)를 사용합니다.
RUN mkdir -p /home/airflow/.cache/huggingface && \
    chown -R 50000:0 /home/airflow/.cache

# 3. 다시 airflow 사용자로 전환하여 파이썬 패키지 관련 작업만 수행
USER airflow

# a. requirements.txt 파일 복사
COPY requirements.txt .

# b. airflow 사용자 권한으로 파이썬 라이브러리 설치
RUN pip install --no-cache-dir -r requirements.txt

# c. airflow 사용자 권한으로 Playwright 브라우저만 다운로드
#    시스템 의존성은 이미 root로 설치했으므로, 여기서는 sudo가 필요 없습니다.
RUN playwright install chrome