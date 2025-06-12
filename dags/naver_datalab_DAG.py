from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import os
import boto3
import snowflake.connector

import urllib.request
import json
import pandas as pd
from datetime import datetime as dt
from itertools import product

dag = DAG(
    dag_id='NaverDatalab',
    start_date=datetime(2025,6,8),
    catchup=False,
    tags=['naver','datalab'],
    schedule='0 0 * * *'   
)
 
def collect_data():
    # 분석 키워드 목록
    # 총 312번 호출
    # space있는 것과 없는 것 수치가 동일 (대선 지지율, 대선지지율로 test)
    # 5.10~11 - 후보등록
    # 5.12~ - 공식 선거운동
    # 5.18 - 후보자 토론회 (초청 1차)
    # 5.23 - 후보자 토론회 (초청 2차)
    # 5.27 - 후보자 토론회 (초청 3차)
    # 5.29~30 - 사전투표
    # 6.3 - 본투표

    keywords = [
        "대선", "대선후보", "대선지지율", "대선공약", "공약", "지지율", "대통령",
        "더불어민주당", "민주당", "국민의힘", "국힘", "개혁신당", "민주노동당",
        "이재명", "이재명공약", "이재명지지율",
        "김문수", "김문수공약", "김문수지지율",
        "이준석", "이준석공약", "이준석지지율",
        "권영국", "권영국공약", "권영국지지율"
    ]

    # 연령대 그룹과 성별
    # ELT로 해야하지만 호출 횟수 때문에 recode를 미리함
    # recode를 미리하지 않으면 하루에 1번밖에 테스트를 못해~~ ㅠㅠ
    # 추후에는 그대로 raw_data에 업로드하고, recode는 analytics 에서 하는 것으로 변경예정
    ages_groups = [
        ["1", "2"], ["3", "4"], ["5", "6"],
        ["7", "8"], ["9", "10"], ["11"]
    ]
    genders = ["f", "m"]

    # 네이버 API 정보
    
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    url = "https://openapi.naver.com/v1/datalab/search"

    # 결과 저장
    results = []
    total_requests = 0

    # keyword × age × gender 조합별로 요청
    # 대선 대략 1달전인 05-01부터 수집 당일까지 데이터 수집
    today = dt.today().strftime('%Y-%m-%d')
    
    for keyword, ages, gender in product(keywords, ages_groups, genders):
        body = {
            "startDate": "2025-05-01",
            "endDate": today,
            "timeUnit": "date",
            "keywordGroups": [
                {
                    "groupName": keyword,
                    "keywords": [keyword]
                }
            ],
            "ages": ages,
            "gender": gender
        }

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)
        request.add_header("Content-Type", "application/json")

        try:
            response = urllib.request.urlopen(request, data=json.dumps(body).encode("utf-8"))
            data = json.loads(response.read().decode("utf-8"))

            for entry in data["results"][0]["data"]:
                results.append({
                    "date": entry["period"],
                    "keyword": keyword,
                    "ratio": entry["ratio"],
                    "gender": gender,
                    "ages": "-".join(ages)
                })

            print(f"{keyword} | {gender} | {ages} 요청 성공")
            total_requests += 1

        except urllib.error.HTTPError as e:
            print(f"{keyword} | {gender} | {ages} 요청 실패 - {e.code}")
            print(e.read().decode())

    print("총 요청 수:", total_requests)
    print("결과 수:", len(results))
    # 결과 csv로 출력
    df = pd.DataFrame(results)
    age_map = {
    '1-2': 1,
    '3-4': 2,
    '5-6': 3,
    '7-8': 4,
    '9-10': 5,
    '11': 6,
    }
    df['gender'] = df['gender'].replace({'m': 1, 'f': 2})
    df['ages'] = df['ages'].astype(str).map(age_map)
    today = dt.today().strftime('%y%m%d')
    #df.to_csv("data/naver_datalab_keywords_"+today+".csv", index=False, encoding="utf-8")
    df.to_csv("/opt/airflow/data/naver_datalab_keywords_"+today+".csv", index=False, encoding="utf-8")

def upload_s3():
    
    S3_BUCKET = 'presivote-bucket'
    S3_FOLDER = 'naver-datalab/'
    
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    s3 = boto3.client('s3')
    
    today = dt.today().strftime('%y%m%d')
    filename = f"naver_datalab_keywords_{today}.csv"
    local_path = f"/opt/airflow/data/{filename}"
    s3_key = f"{S3_FOLDER}{filename}"
    
    print("파일 경로 존재 확인:", os.path.exists(local_path))

    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"S3 업로드 성공: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"S3 업로드 실패: {e}")
        raise

def upload_snowflake():
    
    sf_user = os.getenv("SF_USER")
    sf_password = os.getenv("SF_PASSWORD")
    sf_account = os.getenv("SF_ACCOUNT")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse="COMPUTE_WH",
    database="pre",
    schema="raw_data",
    )
    cs = conn.cursor()

    try:
        today = datetime.today().strftime('%y%m%d')
        s3_path = f"s3://presivote-bucket/naver-datalab/naver_datalab_keywords_{today}.csv"

        # full refresh
        cs.execute("TRUNCATE TABLE pre.raw_data.naver_datalab")
        cs.execute(f"""
            COPY INTO pre.raw_data.naver_datalab
            FROM '{s3_path}'
            credentials=(AWS_KEY_ID='{AWS_ACCESS_KEY_ID}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}')
            FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
        """)

        print("Snowflake 업로드 성공")
    except Exception as e:
        print(f"Snowflake 업로드 실패: {e}")
        raise
    finally:
        cs.close()
        conn.close()
    
def transform_analytics():
    sf_user = os.getenv("SF_USER")
    sf_password = os.getenv("SF_PASSWORD")
    sf_account = os.getenv("SF_ACCOUNT")
    
    conn = snowflake.connector.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse="COMPUTE_WH",
    database="pre",
    schema="analytics",
    )
    cs = conn.cursor()

    try:
        # add label
        cs.execute("""
        CREATE OR REPLACE TABLE analytics.naver_datalab_label AS
        SELECT
            d.date,
            d.keyword,
            d.gender,
            g.label AS gender_label,
            d.ages as age,
            a.label AS age_label,
            d.ratio
        FROM pre.raw_data.naver_datalab d
        LEFT JOIN pre.raw_data.gender g ON d.gender = g.code
        LEFT JOIN pre.raw_data.age a ON d.ages = a.code
    """)

        print("analytics 작업 성공")
    except Exception as e:
        print(f"analytics 작업 실패: {e}")
        raise
    finally:
        cs.close()
        conn.close()
            
collect_data = PythonOperator(
    task_id='collect_data',
    python_callable=collect_data,
    dag=dag)

upload_s3 = PythonOperator(
    task_id='upload_s3',
    python_callable=upload_s3,
    dag=dag)

upload_snowflake = PythonOperator(
    task_id='upload_snowflake',
    python_callable=upload_snowflake,
    dag=dag)

transform_analytics = PythonOperator(
    task_id='transform_analytics',
    python_callable=transform_analytics,
    dag=dag)

collect_data >> upload_s3 >> upload_snowflake >> transform_analytics
    
    

