# 1. Airflow 공식 이미지를 기반으로 시작합니다.
FROM apache/airflow:3.0.3

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# 2. 루트(root) 사용자로 전환하여 모든 시스템 레벨의 작업을 먼저 수행합니다.
USER root
RUN playwright install-deps

# 4. 나중에 airflow 사용자가 사용할 캐시 폴더들을 미리 만들고,
#    전체 홈 디렉토리의 소유권을 airflow 사용자(UID 50000) 및 root 그룹(GID 0)에게 부여합니다.
#    이것이 모든 권한 문제를 근본적으로 해결하는 가장 확실한 방법입니다.
RUN mkdir -p /home/airflow/.cache/pip /home/airflow/.cache/huggingface /home/airflow/.cache/ms-playwright && \
    chown -R 50000:0 /home/airflow

# 5. 이제 airflow 사용자로 최종 전환합니다.
USER airflow

# 6. 사용자 환경 변수를 명시적으로 설정하여 안정성을 높입니다.
ENV HOME=/home/airflow
ENV PATH=/home/airflow/.local/bin:$PATH

# 9. Hugging Face 모델 미리 다운로드하여 이미지에 포함
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-large')"