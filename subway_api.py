"""
    서울시 지하철 일별 승하차 API 데이터를 수집하여
    BigQuery 테이블에 업로드하는 예제 코드
"""

# 1. 라이브러리 불러오기
import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import calendar

# .env 파일의 환경변수 로드
load_dotenv()

# 2. 기본 설정
API_KEY = os.getenv("SUBWAY_API_KEY")

PROJECT_ID = "subway-weather"
DATASET_ID = "subway_dataset"
TABLE_ID = "subway"

# BigQuery 클라이언트 생성
client = bigquery.Client(project=PROJECT_ID)
print("BigQuery 연결 성공:", PROJECT_ID, client)

# 3. 수집할 기간 지정
start_year, start_month, start_day = 2024, 1, 1
end_year, end_month, end_day = 2025, 9, 30

# 4. 데이터 수집
all_data = []

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        # 마지막 월 넘어가면 종료
        if (year == end_year) and (month > end_month):
            break

        last_day = calendar.monthrange(year, month)[1]  # 그 달의 마지막 날짜
        for day in range(1, last_day + 1):
            if (year == end_year and month == end_month and day > end_day):
                break

            use_date = f"{year}{month:02d}{day:02d}"  # YYYYMMDD
            print(f"{use_date} 데이터 수집 중")

            url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/CardSubwayStatsNew/1/1000/{use_date}"

            try:
                res = requests.get(url, timeout=30)
                data = res.json()

                if "CardSubwayStatsNew" not in data:
                    print(f"{use_date}: 데이터 없음")
                    continue

                rows = data["CardSubwayStatsNew"]["row"]
                df = pd.DataFrame(rows)

                # 컬럼 이름 변경
                rename_map = {
                    "USE_YMD": "use_date",
                    "SBWY_ROUT_LN_NM": "line_num",
                    "SBWY_STNS_NM": "station_name",
                    "GTON_TNOPE": "ride_passenger_num",
                    "GTOFF_TNOPE": "alight_passenger_num"
                }
                df = df.rename(columns=rename_map)

                # 날짜 변환
                df["use_date"] = pd.to_datetime(df["use_date"], format="%Y%m%d")

                # 숫자 변환
                for col in ["ride_passenger_num", "alight_passenger_num"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                all_data.append(df)
                print(f"{use_date}: {len(df)} 건 수집 완료")

            except Exception as e:
                print(f"{use_date} 에러 발생: {e}")
                continue

# DataFrame 합치기
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    print("전체 데이터 크기:", final_df.shape)
else:
    print("수집 데이터 없음")
    exit()

# 5. BigQuery 업로드
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("use_date", "DATE"),
        bigquery.SchemaField("line_num", "STRING"),
        bigquery.SchemaField("station_name", "STRING"),
        bigquery.SchemaField("ride_passenger_num", "INTEGER"),
        bigquery.SchemaField("alight_passenger_num", "INTEGER"),
    ],
    write_disposition="WRITE_APPEND",  # 기존 데이터 유지, 신규 데이터만 추가
)

job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
job.result()

print(f"BigQuery 업로드 완료: {table_ref}, {len(final_df)} rows")