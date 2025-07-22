import json
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from collections import Counter
from typing import List
from tqdm import tqdm
import re
from utils import save_json_data, generate_timestamped_filename, load_json_data

# --- 설정 ---
N_CLUSTERS = 200 # 클러스터 개수

# --- 유틸리티 함수 (기존 코드와 동일) ---
def split_job_category(category_str):
    if pd.isna(category_str) or not category_str.strip():
        return []

    tokens = []
    buffer = ""
    parts = re.split(r'(,)', category_str)

    i = 0
    while i < len(parts):
        part = parts[i]
        if part == ',':
            if i + 1 < len(parts) and not parts[i + 1].startswith(' '):
                buffer += ',' + parts[i + 1]
                i += 2
            else:
                tokens.append(buffer.strip())
                buffer = ""
                i += 1
        else:
            buffer += part
            i += 1

    if buffer:
        tokens.append(buffer.strip())

    return tokens

def get_priority_dict(jobs) -> dict:
    all_categories = []
    for job in jobs:
        all_categories.extend(split_job_category(job.get("job_category", "")))
    counts = Counter(all_categories)
    return {k: i for i, (k, _) in enumerate(counts.most_common())}

def get_representative_category(counter: Counter, priority_dict: dict):
    if not counter:
        return None
    max_count = max(counter.values())
    candidates = [k for k, v in counter.items() if v == max_count]
    if len(candidates) == 1:
        return candidates[0]
    return sorted(candidates, key=lambda x: priority_dict.get(x, float('inf')))[0]

# --- Airflow 실행 함수 ---
def cluster_jobs_data(ti):
    """
    Airflow Task: 임베딩된 JSON 파일을 읽어 클러스터링을 수행하고,
    결과를 새로운 JSON 파일로 저장합니다.
    """
    # 1. 이전 embedding_jobs_task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    input_path = ti.xcom_pull(task_ids='embedding_jobs_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 임베딩 데이터 파일 경로를 가져오지 못했습니다.")

    print(f"클러스터링을 위해 데이터 파일을 로드합니다: {input_path}")
    jobs = load_json_data(input_path)
    if not jobs:
        print("클러스터링할 데이터가 없습니다.")
        return None

    embeddings = np.array([job["full_embedding"] for job in jobs])

    # 2. KMeans 클러스터링 수행
    print(f"KMeans 클러스터링을 시작합니다 (n_clusters={N_CLUSTERS})...")
    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=42, n_init="auto")
    cluster_labels = kmeans.fit_predict(embeddings)
    print("클러스터링 완료.")

    # 3. 각 클러스터의 대표 직무 카테고리 추출
    df = pd.DataFrame(jobs)
    df["cluster"] = cluster_labels
    df["job_category_list"] = df["job_category"].apply(split_job_category)

    priority_dict = get_priority_dict(jobs)

    rep_categories = []
    # tqdm을 사용하여 Airflow 로그에 진행률 표시
    for cluster_id in tqdm(range(N_CLUSTERS), desc="클러스터 대표 직무 지정 중"):
        group = df[df["cluster"] == cluster_id]
        merged_categories = [cat for sublist in group["job_category_list"] for cat in sublist]
        counter = Counter(merged_categories)
        rep = get_representative_category(counter, priority_dict)
        rep_categories.append((cluster_id, rep))

    rep_dict = {cid: rep for cid, rep in rep_categories}

    df["representative_category"] = df["cluster"].map(rep_dict)
    
    # 4. 원본 jobs 데이터에 클러스터링 결과 추가
    final_jobs = df.to_dict(orient='records')
    for i in range(len(final_jobs)):
        # numpy int64 타입을 파이썬 기본 int로 변환
        final_jobs[i]["cluster"] = int(final_jobs[i]["cluster"])
        # 임베딩 데이터는 최종 파일에서 제외 (용량 문제)
        # del final_jobs[i]["full_embedding"]
        del final_jobs[i]["job_category_list"]
        
    # 5. 결과를 새로운 JSON 파일로 저장
    filename = generate_timestamped_filename("clustered_jobs")
    file_path = save_json_data(final_jobs, filename)

    print(f"클러스터링 및 대표 직무 지정 완료! 데이터를 새 파일로 저장했습니다: {file_path}")
    return file_path