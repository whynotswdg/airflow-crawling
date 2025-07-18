# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.3

# 2. 파이썬 라이브러리 목록 복사 및 설치 (airflow 사용자 권한으로)
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Playwright 설치
# 루트 권한으로 시스템 의존성 설치
USER root
# 'install chrome --with-deps'가 더 효율적이지만, 기존 방식을 유지해도 괜찮습니다.
RUN playwright install-deps 

# 다시 airflow 사용자로 전환하여 브라우저 다운로드
USER airflow
RUN playwright install chrome # <-- 어떤 브라우저를 설치할지 'chrome'으로 명시하는 것이 좋습니다.

# 4. [추가] Hugging Face 캐시 폴더 권한 문제 해결
# 루트 권한으로 폴더 생성 및 권한 부여
USER root
RUN mkdir -p /home/airflow/.cache/huggingface && \
    chown -R airflow:airflow /home/airflow/.cache

# 5. 최종적으로 컨테이너의 기본 사용자를 airflow로 설정
USER airflow