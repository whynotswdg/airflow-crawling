# 1. Airflow 공식 이미지를 기반으로 시작합니다. (최신 버전 확인)
FROM apache/airflow:3.0.3

# 2. requirements.txt 복사 및 라이브러리 설치 (airflow 사용자 권한)
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Playwright 브라우저 설치 (airflow 사용자 권한)
RUN playwright install chrome --with-deps

# 4. Hugging Face 모델 미리 다운로드하여 이미지에 포함 (airflow 사용자 권한)
#    - sentence-transformers 라이브러리를 사용하여 모델을 기본 캐시 위치에 다운로드합니다.
#    - 이렇게 하면 DAG 실행 시에는 다운로드 없이 바로 로드할 수 있습니다.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-large')"

# 5. (선택사항, 안정성을 위해) 루트 사용자로 캐시 폴더 권한을 다시 한번 확인
USER root
RUN chown -R airflow:airflow /home/airflow/.cache
USER airflow