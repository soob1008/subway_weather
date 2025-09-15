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

# 월별 데이터 누적
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
    # 서버가 늦게 응답하거나 멈춰도 최대 30초만 기다리고 Timeout 예외를 내서 프로그램이 안멈춘다.
    res = requests.get(url, timeout=30)
    # 응답 코드가 4xx/5xx(에러)이면 바로 예외(HTTPError)를 던짐.
    # 패 응답을 다음 단계로 넘기지 않고 초기에 실패를 감지해서 디버깅/재시도 처리를 쉽게 하려고 쓴다.
    res.raise_for_status()
    # res.text는 이 encoding을 써서 바이트 → 문자열로 디코딩하기 때문. (한글 깨짐 방지)
    res.encoding = "utf-8"
    # 디코딩된 본문 문자열을 가져와서 앞뒤 공백/개행을 제거.
    raw_text = res.text.strip()

    # 3. 응답 파싱
    lines = raw_text.split("\n")

    header_line = None
    for line in lines:
        if line.startswith("#") and "YYMMDDHHMI" in line:
            # lstrip("#") : 문자열 왼쪽에 붙은 #들은 모두 제거
            # split : 'YYMMDDHHMI STN TA RN HM' → ['YYMMDDHHMI','STN','TA','RN','HM']
            header_line = line.lstrip("#").strip().split()
            break

    data_lines = [line for line in lines if line and line[0].isdigit()]

    if not header_line or not data_lines:
        print(f"{year}-{month:02d} 응답 없음(건너뜀)")
    else:
        data = [line.split() for line in data_lines]
        df_m = pd.DataFrame(data, columns=header_line[:len(data[0])])

        # 4. 필요한 컬럼 추출 + 리네이밍
        rename_map = {
            "YYMMDDHHMI": "datetime",   # 관측시각
            "STN": "station_id",        # 관측소 ID
            "TA": "temperature",        # 기온
            "RN": "rainfall",           # 시간 강수량 (mm)
            "HM": "humidity"            # 습도
        }

        df_m = df_m.rename(columns=rename_map)
        # 중복된 컬럼명을 제거해서, 유일한 컬럼만 남기는 코드
        df_m = df_m.loc[:, ~df_m.columns.duplicated()]
        df_m = df_m[list(rename_map.values())]

        # 5. 자료형 변환
        # astype 팬더스에서 자료형을 바꾸는 메서드
        df_m["datetime"] = df_m["datetime"].astype(str)
        df_m["station_id"] = pd.to_numeric(df_m["station_id"], errors="coerce").astype("Int64")
        df_m["rainfall"] = pd.to_numeric(df_m["rainfall"], errors="coerce")
        # pandas에서 “라벨(이름)로 행/열을 선택하고(또는 바꾸는) 인덱서
        df_m.loc[df_m["rainfall"] < 0, "rainfall"] = 0

        for col in ["temperature", "rainfall", "humidity"]:
            if col not in df_m.columns:
                df_m[col] = None
            df_m[col] = pd.to_numeric(df_m[col], errors="coerce")

        dfs.append(df_m)
        print(f" {year}-{month:02d} rows: {len(df_m)}")

    # 다음 달로 이동
    month += 1
    if month == 13:
        month = 1
        year += 1
    # 2. API 호출
    url = (
        f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php"
        f"?tm1={tm1}&tm2={tm2}&stn={STATION_ID}&authKey={API_KEY}"
    )
    # 서버가 늦게 응답하거나 멈춰도 최대 30초만 기다리고 Timeout 예외를 내서 프로그램이 안멈춘다.
    res = requests.get(url, timeout=30)
    # 응답 코드가 4xx/5xx(에러)이면 바로 예외(HTTPError)를 던짐.
    # 패 응답을 다음 단계로 넘기지 않고 초기에 실패를 감지해서 디버깅/재시도 처리를 쉽게 하려고 쓴다.
    res.raise_for_status()
    # res.text는 이 encoding을 써서 바이트 → 문자열로 디코딩하기 때문. (한글 깨짐 방지)
    res.encoding = "uft-8"
    # 디코딩된 본문 문자열을 가져와서 앞뒤 공백/개행을 제거.
    raw_text = res.text.strip()

    # 3. 응답 파싱
    lines = raw_text.split("\n")

    header_line = None
    for line in lines:
        if line.startswith("#") and "YYMMDDHHMI" in line:
            # lstrip("#") : 문자열 왼쪽에 붙은 #들은 모두 제거
            # split : 'YYMMDDHHMI STN TA RN HM' → ['YYMMDDHHMI','STN','TA','RN','HM']
            header_line = line.lstrip("#").strip().split()
            break

    data_lines = [line for line in lines if line and line[0].isdigit()]

    if not header_line or not data_lines:
        print(f"{year}-{month:02d} 응답 없음(건너뜀)")
    else:
        data = [line.split() for line in data_lines]
        df_m = pd.DataFrame(data, columns=header_line[:len(data[0])])

        # 4. 필요한 컬럼 추출 + 리네이밍
        rename_map = {
            "YYMMDDHHMI": "datetime",   # 관측시각
            "STN": "station_id",        # 관측소 ID
            "TA": "temperature",        # 기온
            "RN": "rainfall",           # 시간 강수량 (mm)
            "HM": "humidity"            # 습도
        }

        df_m = df_m.rename(columns=rename_map)
        # 중복된 컬럼명을 제거해서, 유일한 컬럼만 남기는 코드
        df_m = df_m.loc[:, ~df_m.columns.duplicated()]
        df_m = df_m[list(rename_map.values())]

        # 5. 자료형 변환
        # astype 팬더스에서 자료형을 바꾸는 메서드
        df_m["datetime"] = df_m["datetime"].astype(str)
        df_m["station_id"] = pd.to_numeric(df_m["station_id"], errors="coerce").astype("Int64")
        df_m["rainfall"] = pd.to_numeric(df_m["rainfall"], errors="coerce")
        # pandas에서 “라벨(이름)로 행/열을 선택하고(또는 바꾸는) 인덱서
        df_m.loc[df_m["rainfall"] < 0, "rainfall"] = 0

        for col in ["temperature", "rainfall", "humidity"]:
            if col not in df_m.columns:
                df_m[col] = None
            df_m[col] = pd.to_numeric(df_m[col], errors="coerce")

        dfs.append(df_m)
        print(f" {year}-{month:02d} rows: {len(df_m)}")

    # 다음 달로 이동
    month += 1
    if month == 13:
        month = 1
        year += 1

# 6. 전체 합치기 + 중복 제거
if not dfs:
    # raise 강제 오류를 발생 시킨다.
    raise RuntimeError("수집된 데이터가 없습니다. API 키/기간/지점 확인 필요.")

# 여러개 pd를 하나로 합치고, drop_duplicates를 통해 중복된 행을 제거 한다.
df = pd.concat(dfs, ignore_index=True).drop_duplicates(
    subset=["datetime", "station_id"], keep="last"
)

print("전체 합친 데이터 샘플")
print(df.head())
print("총 행수:", len(df))

# 7. BigQuery 업로드
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"


# 빅쿼리는 데이터를 올릴 때 Load Job 적재 작업 이라는걸 실행한다.
# LocalJobConfig 는 그 작업을 어떻게 할지 설정하는 객체이다.
job_config = bigquery.LoadJobConfig(
    schema = [
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