import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

API_KEY = "I9zn-apKQjec5_mqSkI3nQ"

PROJECT_ID = "subway-weather"
DATASET_ID = "weather_dataset"
TABLE_ID = "weather"
STATION_ID = 108
client = bigquery.Client(project=PROJECT_ID)
# ==============================
# 2. API 호출
# ==============================
tm1 = "202401010100"   # 시작일시 (YYYYMMDDHHMM) → 2024-01-01 01시
tm2 = "202509302300"   # 종료일시 (YYYYMMDDHHMM) → 2025-09-30 23시

url = (
    f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"
    f"?tm1={tm1}&tm2={tm2}&stn={STATION_ID}&authKey={API_KEY}"
)

res = requests.get(url)
res.encoding = "utf-8"
raw_text = res.text.strip()


# ==============================
# 3. 응답 파싱
# ==============================
lines = raw_text.split("\n")

# 1. 헤더 줄 찾기 (#로 시작하는 줄 중 첫 번째)
header_line = None
for line in lines:
    if line.startswith("#") and "YYMMDDHHMI" in line:
        header_line = line.lstrip("#").strip().split()
        break

# 2. 데이터 줄 추출 (숫자로 시작하는 줄만)
data_lines = [line for line in lines if line and line[0].isdigit()]

# 3. DataFrame 생성
data = [line.split() for line in data_lines]
df = pd.DataFrame(data, columns=header_line[:len(data[0])])


# ==============================
# 4. 필요한 컬럼 추출 + 리네이밍
# ==============================
rename_map = {
    "YYMMDDHHMI": "datetime",   # 관측시각 (YYYYMMDDHHMM)
    "STN": "station_id",        # 관측소 ID
    "TA": "temperature",        # 기온 (℃)
    "RN": "rainfall",           # 시간 강수량 (mm)
    "HM": "humidity"            # 습도 (%)
}
df = df.rename(columns=rename_map)
df = df.loc[:, ~df.columns.duplicated()]

# 필요한 컬럼만 유지
df = df[list(rename_map.values())]

# ==============================
# 5. 자료형 변환
# ==============================
df["datetime"] = df["datetime"].astype(str)
df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce").astype("Int64")

for col in ["temperature", "rainfall", "humidity"]:
    if col not in df.columns:
        df[col] = None  # 컬럼 추가
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ==============================
# 5. 자료형 변환
# ==============================
df["datetime"] = df["datetime"].astype(str)
df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce").astype("Int64")
df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
df["rainfall"] = pd.to_numeric(df["rainfall"], errors="coerce")
df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

print("📊 날씨 데이터 샘플:")
print(df.head())

# ==============================
# 6. BigQuery 업로드
# ==============================
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("station_id", "INTEGER"),
        bigquery.SchemaField("temperature", "FLOAT"),
        bigquery.SchemaField("rainfall", "FLOAT"),
        bigquery.SchemaField("humidity", "FLOAT"),
    ],
    write_disposition="WRITE_TRUNCATE"  # 매 실행마다 덮어쓰기 (누적 원하면 WRITE_APPEND)
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()

print(f"🎉 BigQuery 업로드 완료: {table_ref}, {len(df)} rows")