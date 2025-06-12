
# 대한민국 대선 트렌드 분석 시각화

본 프로젝트는 2025년 제21대 대통령 선거를 앞두고, 대중의 관심사 변화를 다양한 데이터 소스를 통해 분석하고 시각화하는 것을 목표로 합니다.

## 📌 프로젝트 목표
- 선거 기간 대중의 관심사를 시각화한 대시보드 제공
- End-to-end 데이터 파이프라인 구축
- Docker, Airflow, Snowflake 등 데이터 엔지니어링 역량 강화

## 🗃️ 사용 데이터
- Naver DataLab API
- Google Trends (Pytrends)
- 네이버 뉴스

## 🛠️ 사용 기술
- **Language**: Python, SQL
- **Container**: Docker
- **Workflow**: Apache Airflow
- **Storage**: Amazon S3, Snowflake
- **Visualization**: Apache Superset
- **Collaboration**: Slack, ZEP, Notion, Git
- **IDE**: Visual Studio Code

## 🧩 DAG 구성
- Naver Datalab DAG
- Google Trends DAG
- Naver News DAG

## 🧠 기대효과
- 다차원적 대선 트렌드 파악
- 정책 및 전략 수립에 유의미한 인사이트 제공
- 자동화된 데이터 처리로 운영 효율성 강화

## 🔧 향후 개선 방향
- CI/CD 파이프라인 구축
- dbt를 통한 데이터 모델링 체계화
- 예측 기반 트렌드 분석 도입
