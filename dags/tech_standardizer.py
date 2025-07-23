def get_tech_stack_map():
    """
    기술 스택 표준화를 위한 규칙 사전을 반환합니다.
    - Key: 표준화될 최종 기술 이름
    - Value: 표준화될 대상이 되는 다양한 표기법 리스트 (모두 소문자로 작성)
    """
    return {
        "AWS": ["aws", "aws s3", "s3", "rds", "ec2", "lambda", "ecs", "eks", "aurora", "dynamodb", "cloudfront", "sagemaker", "elasticache", "cloudformation", "sqs", "emr", "iam", "vpc", "aws (ec2", "rds)"],
        "GCP": ["gcp", "google cloud platform", "google cloud", "bigquery", "firebase", "gke", "cloud run", "cloudsql", "google cloud service", "vertex ai", "firestore"],
        "Azure": ["azure", "microsoft azure", "azure devops", "azure iot"],
        "Docker": ["docker", "docker compose"],
        "Kubernetes": ["kubernetes", "k8s", "kubeflow"],
        "CI/CD": ["ci/cd", "jenkins", "github actions", "gitlab ci", "circleci", "argocd", "argo cd", "spinnaker", "teamcity", "travis ci", "bamboo"],
        "Git": ["git", "github"],
        "Java": ["java"],
        "Spring": ["spring", "spring framework", "spring boot", "springboot", "spring mvc", "spring data jpa", "spring batch", "spring security", "spring cloud"],
        "Python": ["python", "py", "파이썬"],
        "Django": ["django", "drf", "django rest framework"],
        "FastAPI": ["fastapi"],
        "Flask": ["flask"],
        "Node.js": ["node.js", "nodejs", "node"],
        "Express": ["express", "express.js"],
        "NestJS": ["nestjs", "nest.js"],
        "Go": ["go", "golang"],
        "Kotlin": ["kotlin"],
        "C#": ["c#", ".net", ".net core", "asp.net"],
        "C++": ["c++", "c/c++"],
        "C": ["c"],
        "JavaScript": ["javascript", "js", "es6", "ecmascript"],
        "TypeScript": ["typescript", "ts"],
        "React": ["react", "react.js", "reactjs", "react native", "react-native", "react query", "react-query", "redux", "recoil", "zustand", "mobx"],
        "Vue.js": ["vue.js", "vue", "vuejs", "vuex", "pinia", "nuxt.js"],
        "Next.js": ["next.js", "nextjs"],
        "HTML": ["html", "html5"],
        "CSS": ["css", "css3", "scss", "sass", "tailwind css", "tailwindcss"],
        "SQL": ["sql", "mysql", "postgresql", "postgres", "oracle", "mariadb", "mssql", "ms-sql", "rdbms", "rdb", "sql server", "microsoft sql server", "sqlite"],
        "NoSQL": ["nosql", "mongodb", "mongo", "redis", "elasticsearch", "elastic search", "opensearch", "cassandra", "dynamodb"],
        "JPA": ["jpa", "hibernate"],
        "PyTorch": ["pytorch", "torch"],
        "TensorFlow": ["tensorflow"],
        "Kafka": ["kafka", "apache kafka"],
        "Spark": ["spark", "apache spark"],
        "Airflow": ["airflow", "apache airflow"],
        # ... 필요에 따라 계속 추가 ...
    }

def standardize_tech_list(raw_list: list, tech_map: dict) -> list:
    """
    단어 리스트를 받아 표준화된 기술 스택 리스트를 반환합니다.
    """
    if not raw_list:
        return []

    standardized_set = set()
    
    for item in raw_list:
        stack = item.strip().lower()
        if not stack:
            continue
            
        found = False
        for standard_name, variations in tech_map.items():
            if stack in variations:
                standardized_set.add(standard_name)
                found = True
                break
        if not found:
            standardized_set.add(stack.title())

    return sorted(list(standardized_set))