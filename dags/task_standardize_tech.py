import json

def get_tech_stack_map():
    """
    기술 스택 표준화를 위한 규칙 사전을 반환합니다.
    - Key: 표준화될 최종 기술 이름
    - Value: 표준화될 대상이 되는 다양한 표기법 리스트 (모두 소문자로 작성)
    """
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

def standardize_tech_stacks(raw_stacks_str: str | None) -> str | None:
    """LLM이 추출한 기술 스택 문자열을 받아, 정의된 규칙에 따라 최종 표준화합니다."""
    if not raw_stacks_str:
        return None

    tech_map = get_tech_stack_map()
    standardized_set = set()
    
    # 쉼표로 구분된 문자열을 개별 기술 리스트로 변환
    raw_stacks = [stack.strip().lower() for stack in raw_stacks_str.split(',')]

    for stack in raw_stacks:
        if not stack:  # 빈 문자열은 건너뛰기
            continue
            
        found_and_mapped = False
        # 표준화 사전을 순회하며 매핑되는 대표 이름 찾기
        for standard_name, variations in tech_map.items():
            if stack in variations:
                standardized_set.add(standard_name)
                found_and_mapped = True
                break
        
        # 매핑되는 규칙이 없는 경우, 첫 글자를 대문자로 바꿔서 그대로 추가
        if not found_and_mapped:
            standardized_set.add(stack.title())

    if not standardized_set:
        return None

    return ", ".join(sorted(list(standardized_set)))

# --- 실행 예시 ---
# LLM이 아래와 같이 다양한 형태로 결과를 반환했다고 가정
example_input = "React.js, k8s, 파이썬, aws ec2, HTML5, SpringBoot, nextjs, rds, elastic search, git, Github"
standardized_output = standardize_tech_stacks(example_input)

print(f"입력: {example_input}")
print(f"결과: {standardized_output}")
# 예상 출력: AWS, Git, HTML, Kubernetes, Next.js, Python, React, Spring