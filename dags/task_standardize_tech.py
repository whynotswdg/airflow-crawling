import json
# 'utils'와 같은 다른 로컬 모듈을 사용하기 위해 import 경로를 명확히 합니다.
from utils import save_json_data, generate_timestamped_filename, load_json_data

def get_tech_stack_map():
    """
    기술 스택 표준화를 위한 규칙 사전을 반환합니다.
    - Key: 표준화될 최종 기술 이름
    - Value: 표준화될 대상이 되는 다양한 표기법 리스트 (모두 소문자로 작성)
    """
    # 사용자가 제공한 tech_map 딕셔너리 (내용은 생략)
    return {
        # --- 클라우드 & DevOps ---
        "AWS": [
            "aws", "aws s3", "s3", "aws rds", "rds", "aws ec2", "ec2", "aws lambda", "lambda", 
            "aws ecs", "ecs", "aws eks", "eks", "amazondocumentdb", "aws aurora", "aurora", 
            "dynamodb", "aws glue", "elastic beanstalk", "cloudfront", "aws cloudfront",
            "codepipeline", "aws codepipeline", "sagemaker", "aws sagemaker", "elasticache",
            "cloudformation", "aws cloudformation", "sqs", "aws sqs", "emr", "aws emr", "iam",
            "vpc", "aws (ec2", "rds)"
        ],
        "GCP": [
            "gcp", "google cloud platform", "google cloud", "bigquery", "firebase", 
            "google kubernetes engine", "gke", "cloud run", "cloudsql", "google cloud service", 
            "vertex ai", "firestore"
        ],
        "Azure": ["azure", "microsoft azure", "azure devops", "azure iot"],
        "Docker": ["docker", "docker compose"],
        "Kubernetes": ["kubernetes", "k8s", "k8s", "kubeflow"],
        "CI/CD": [
            "ci/cd", "jenkins", "github actions", "gitlab ci", "circleci", "argocd", "argo cd",
            "spinnaker", "teamcity", "travis ci", "bamboo"
        ],
        "Terraform": ["terraform"],
        "Ansible": ["ansible"],
        "Jenkins": ["jenkins"],
        "Git": ["git", "github"], # Git과 GitHub을 'Git'으로 통일

        # --- 백엔드 ---
        "Java": ["java"],
        "Spring": [
            "spring", "spring framework", "spring boot", "springboot", "spring mvc",
            "spring data jpa", "spring batch", "spring security", "spring cloud"
        ],
        "Python": ["python", "py", "파이썬"],
        "Django": ["django", "drf", "django rest framework"],
        "FastAPI": ["fastapi"],
        "Flask": ["flask"],
        "Node.js": ["node.js", "nodejs", "node"],
        "Express": ["express", "express.js"],
        "NestJS": ["nestjs", "nest.js"],
        "Go": ["go", "golang"],
        "Kotlin": ["kotlin"],
        "Rust": ["rust"],
        "PHP": ["php"],
        "Ruby": ["ruby", "ruby on rails"],
        "C#": ["c#", ".net", ".net core", "asp.net"],
        "C++": ["c++", "c/c++"],
        "C": ["c"],

        # --- 프론트엔드 ---
        "JavaScript": ["javascript", "js", "es6", "ecmascript"],
        "TypeScript": ["typescript", "ts"],
        "React": [
            "react", "react.js", "reactjs", "react native", "react-native", "react query", 
            "react-query", "redux", "recoil", "zustand", "mobx"
        ],
        "Vue.js": ["vue.js", "vue", "vuejs", "vuex", "pinia", "nuxt.js"],
        "Angular": ["angular", "angularjs"],
        "Next.js": ["next.js", "nextjs"],
        "HTML": ["html", "html5"],
        "CSS": ["css", "css3", "scss", "sass", "tailwind css", "tailwindcss"],
        "Webpack": ["webpack"],
        "Vite": ["vite"],

        # --- 데이터베이스 ---
        "SQL": [
            "sql", "mysql", "postgresql", "postgres", "oracle", "mariadb", "mssql", "ms-sql",
            "rdbms", "rdb", "sql server", "microsoft sql server", "sqlite"
        ],
        "NoSQL": [
            "nosql", "mongodb", "mongo", "redis", "elasticsearch", "elastic search",
            "opensearch", "cassandra", "dynamodb"
        ],
        "JPA": ["jpa", "hibernate"],
        "GraphQL": ["graphql"],
        
        # --- AI/ML ---
        "PyTorch": ["pytorch", "torch"],
        "TensorFlow": ["tensorflow"],
        "Keras": ["keras"],
        "scikit-learn": ["scikit-learn", "scikitlearn"],
        "Pandas": ["pandas"],
        "NumPy": ["numpy"],
        "OpenCV": ["opencv"],
        "LangChain": ["langchain"],
        "LLM": ["llm"],

        # --- 기타 툴 & 플랫폼 ---
        "Jira": ["jira"],
        "Confluence": ["confluence"],
        "Slack": ["slack"],
        "Notion": ["notion"],
        "Figma": ["figma"],
        "Kafka": ["kafka", "apache kafka"],
        "RabbitMQ": ["rabbitmq"],
        "Spark": ["spark", "apache spark"],
        "Airflow": ["airflow", "apache airflow"]
    }

def standardize_tech_stacks(raw_stacks_str: str | None, tech_map: dict) -> str | None:
    """LLM이 추출한 기술 스택 문자열을 받아, 정의된 규칙에 따라 최종 표준화합니다."""
    if not raw_stacks_str:
        return None

    standardized_set = set()
    raw_stacks = [stack.strip().lower() for stack in raw_stacks_str.split(',')]

    for stack in raw_stacks:
        if not stack:
            continue
            
        found_and_mapped = False
        for standard_name, variations in tech_map.items():
            if stack in variations:
                standardized_set.add(standard_name)
                found_and_mapped = True
                break
        
        if not found_and_mapped:
            standardized_set.add(stack.title())

    if not standardized_set:
        return None

    return ", ".join(sorted(list(standardized_set)))


def standardize_and_save_data(ti):
    """
    Airflow Task: LLM이 처리한 JSON 파일을 읽어 기술 스택을 최종 표준화하고,
    그 결과를 새로운 JSON 파일로 저장합니다.
    """
    # 1. 이전 preprocess_data_task가 XCom으로 넘겨준 파일 경로를 가져옵니다.
    input_path = ti.xcom_pull(task_ids='preprocess_data_task', key='return_value')
    if not input_path:
        raise ValueError("XCom으로부터 이전 단계의 파일 경로를 가져오지 못했습니다.")

    print(f"최종 표준화할 데이터 파일을 로드합니다: {input_path}")
    data_to_process = load_json_data(input_path)
    if not data_to_process:
        print("표준화할 데이터가 없습니다.")
        return None

    final_data = []
    tech_map = get_tech_stack_map() # 표준화 사전을 한 번만 로드
    print(f"총 {len(data_to_process)}개 데이터의 기술 스택 최종 표준화를 시작합니다...")

    for job in data_to_process:
        raw_tech_stack = job.get("tech_stack")
        # standardize_tech_stacks 함수에 두 인자를 모두 정확하게 전달합니다.
        standardized_stack = standardize_tech_stacks(raw_tech_stack, tech_map)
        job["tech_stack"] = standardized_stack
        final_data.append(job)

    # 2. 최종 데이터를 새 파일로 저장
    if final_data:
        filename = generate_timestamped_filename("final_wanted_jobs")
        file_path = save_json_data(final_data, filename)
        print(f"최종 표준화 완료! 데이터를 {file_path} 파일로 저장했습니다.")
        return file_path
    else:
        print("표준화 후 데이터가 없습니다.")
        return None

# --- 아래 코드는 터미널에서 `python task_standardize_tech.py`로 직접 실행할 때만 작동 ---
if __name__ == '__main__':
    example_input = "React.js, k8s, 파이썬, aws ec2, HTML5, SpringBoot, nextjs, rds, elastic search, git, Github"
    # standardize_tech_stacks 함수를 호출할 때 두 번째 인자로 tech_map을 전달합니다.
    standardized_output = standardize_tech_stacks(example_input, get_tech_stack_map())
    
    print("--- 테스트 실행 ---")
    print(f"입력: {example_input}")
    print(f"결과: {standardized_output}")
    print("-------------------")