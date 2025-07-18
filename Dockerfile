# 1. 베이스 이미지 지정
FROM apache/airflow:3.0.3

# 2. 루트 사용자로 전환하여 모든 시스템 레벨의 작업을 먼저 수행
USER root

# a. requirements.txt 파일 복사
#    pip 설치를 먼저 하는 것이 더 효율적인 캐시 활용을 가능하게 합니다.
COPY requirements.txt .

# b. airflow 사용자 권한으로 파이썬 라이브러리 설치
#    USER를 변경하여 Airflow의 보안 정책을 준수합니다.
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# c. 다시 루트 사용자로 전환하여 Playwright 설치
USER root

# d. --with-deps 옵션을 사용하여 브라우저와 모든 시스템 의존성을 한 번에 설치합니다.
#    이것이 sudo 오류를 해결하는 가장 중요한 핵심입니다.
RUN playwright install chrome --with-deps

# e. Hugging Face 캐시 폴더 생성 및 airflow 사용자에게 소유권 부여
RUN mkdir -p /home/airflow/.cache/huggingface && \
    chown -R 50000:0 /home/airflow/.cache

# 4. 모든 설치가 끝난 후, 컨테이너의 최종 실행 사용자를 airflow로 설정
USER airflow