import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os, calendar

load_dotenv()
API_KEY = os.getenv("WEATHER_API_KEY")

PROJECT_ID = "subway-weather"
DATASET_ID = "weather_dataset"
TABLE_ID = "weather"
STATION_ID = 108

client = bigquery.Client(project=PROJECT_ID)

# 1. 기간 (월 단위) 정의
start_year, start_month = 2024, 1
end_year, end_month = 2025, 9

dfs = []

year, month = start_year, start_month
while (year < end_year) or (year == end_year and month <= end_month):
    last_day = calendar.monthrange(year, month)[1]
    tm1 = f"{year}{month:02d}010000"
    tm2 = f"{year}{month:02d}{last_day:02d}2359"

    # 2. API 호출
    url = (
        f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"
        f"?tm1={tm1}&tm2={tm2}&stn={STATION_ID}&authKey={API_KEY}"
    )
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        res.encoding = "utf-8"
        raw_text = res.text.strip()

        # 3. 응답 파싱
        lines = raw_text.split("\n")
        header_line = None
        for line in lines:
            if line.startswith("#") and "YYMMDDHHMI" in line:
                header_line = line.lstrip("#").strip().split()
                break

        data_lines = [line for line in lines if line and line[0].isdigit()]

        if not header_line or not data_lines:
            print(f"{year}-{month:02d} 응답 없음(건너뜀)")
        else:
            data = [line.split() for line in data_lines]
            df_m = pd.DataFrame(data, columns=header_line[:len(data[0])])

            rename_map = {
                "YYMMDDHHMI": "datetime",
                "STN": "station_id",
                "TA": "temperature",
                "RN": "rainfall",
                "HM": "humidity"
            }

            df_m = df_m.rename(columns=rename_map)
            df_m = df_m.loc[:, ~df_m.columns.duplicated()]
            df_m = df_m[list(rename_map.values())]

            # 타입 변환
            df_m["datetime"] = df_m["datetime"].astype(str)
            df_m["station_id"] = pd.to_numeric(df_m["station_id"], errors="coerce").astype("Int64")
            df_m["rainfall"] = pd.to_numeric(df_m["rainfall"], errors="coerce")
            df_m.loc[df_m["rainfall"] < 0, "rainfall"] = 0

            for col in ["temperature", "rainfall", "humidity"]:
                if col not in df_m.columns:
                    df_m[col] = None
                df_m[col] = pd.to_numeric(df_m[col], errors="coerce")

            dfs.append(df_m)
            print(f"{year}-{month:02d} rows: {len(df_m)}")

    except Exception as e:
        print(f"{year}-{month:02d} 에러 발생: {e}")

    # 4. 다음 달로 이동
    month += 1
    if month == 13:
        month = 1
        year += 1

# 5. 전체 합치기 + 중복 제거
if not dfs:
    raise RuntimeError("수집된 데이터가 없습니다.")

df = pd.concat(dfs, ignore_index=True).drop_duplicates(
    subset=["datetime", "station_id"], keep="last"
)

print("총 행수:", len(df))

# 6. BigQuery 업로드
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("station_id", "INTEGER"),
        bigquery.SchemaField("temperature", "FLOAT"),
        bigquery.SchemaField("rainfall", "FLOAT"),
        bigquery.SchemaField("humidity", "FLOAT")
    ],
    write_disposition="WRITE_TRUNCATE"
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()
print(f"BigQuery 업로드 완료: {table_ref}, {len(df)} rows")