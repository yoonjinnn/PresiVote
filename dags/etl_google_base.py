import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from pytrends.request import TrendReq
from selenium import webdriver
import time

def _get_cookie():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get("https://trends.google.com/")
    time.sleep(5)
    cookie = driver.get_cookie("NID")["value"]
    driver.quit()
    return cookie

def upload_data_to_s3(df: pd.DataFrame, bucket_name: str, key: str, aws_conn_id: str = 'aws_default'):
    if df.empty:
        return
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    csv_buffer = df.to_csv(index=False, encoding='utf-8-sig')

    try:
        s3_hook.load_string(
            string_data=csv_buffer,
            key=key,
            bucket_name=bucket_name,
            replace=True
        )
    except Exception as e:
        raise

def get_google_trends_data(keyword_list: list[str], timeframe: str, geo: str) -> pd.DataFrame:
    try:
        nid_cookie = f"NID={_get_cookie()}"
        pytrds = TrendReq(hl='ko', tz=540,requests_args={"headers": {"Cookie": nid_cookie}})
        pytrds.build_payload(kw_list=keyword_list, timeframe=timeframe, geo=geo)
        df = pytrds.interest_over_time()
        df = df.reset_index()
        interest_over_time_df = df.rename(columns={'index': 'Date'})
        if 'isPartial' in interest_over_time_df.columns:
            interest_over_time_df = interest_over_time_df.drop(columns=['isPartial'])
        return interest_over_time_df
    except Exception as e:
        print(f"Error fetching Google Trends data: {e}")
        raise

def fetch_and_upload_trends_data(**kwargs):
    ti = kwargs['ti']
    keyword_list = kwargs['keyword_list']
    timeframe = kwargs['timeframe']
    geo = kwargs['geo']
    
    trends_df = get_google_trends_data(keyword_list, timeframe, geo)
    
    s3_bucket = "presivote-bucket"
    s3_prefix = "google/"

    execution_date_nodash = kwargs['ds_nodash']
    file_name = f"{execution_date_nodash}_candidates_trends.csv"
    s3_key = f"{s3_prefix}{file_name}"
    aws_connection_id = 'my_s3_connection'

    upload_data_to_s3(trends_df, s3_bucket, s3_key, aws_connection_id)