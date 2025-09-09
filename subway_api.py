"""
    서울시 지하철 시간대별 승하차 API 데이터를 수집하여
    BigQuery 테이블에 업로드하는 예제 코드
"""

# 1. 라이브러리 불러오기
import requests
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os

# 파일에 적어둔 환경변수들을 파이썬 프로세스의 환경변수로 불러오는 함수
load_dotenv()

# 2. 기본 설정
API_KEY = os.getenv("SUBWAY_API_KEY")

PROJECT_ID = "subway-weather"
DATASET_ID = "subway_dataset"
TABLE_ID = "subway_usage"

# BigQuery 클라이언트 생성
client = bigquery.Client(project=PROJECT_ID)

print("BigQuery 연결 성공:", PROJECT_ID, client)

# 3. 수집할 기간 지정
start_year, start_month = 2024, 1
end_year, end_month = 2025, 9

# 4. 데이터 수집

# 월별 데이터를 모두 모아두는 리스트
all_data= []

for year in range(start_year, end_year + 1):
    for month in range(1,13):
        # 마지막 월 까지만 수집
        if(year == end_year) and (month > end_month):
            break

        year_month = f"{year}{month:02d}"
        print(f" {year_month} 데이터 수집 중")

        # API URL 구성
        url = f"http://openapi.seoul.go.kr:8088/${API_KEY}/json/CardSubwayTime/1/1000/{year_month}"

        try:
            res = requests.get(url)
            data = res.json()

            # 데이터 확인
            if "CardSubwayTime" not in data:
                print(f"{year_month}: 데이터 없음")
                continue

            rows = data["CardSubwayTime"]["row"]
            df = pd.DataFrame(rows)

            # 연월 기록 추가
            df["USE_MON"] = year_month

            # 문자열 숫자를 정수로 변환하기
            for col in df.columns:
                if col.endswith("_RIDE_NUM") or col.endswith("_ALIGHT_NUM"):
                    # errors 는 숫자로 바꾸다가 문제가 생겼을 때 어떻게 할지 정하는 옵션
                    # errors="raise"(기본값) : 숫자로 못바꾸는 값이 하나라도 있으면 에러를 내고 중단
                    # errors="coerce" : 숫자로 못 바꾸는 값은 NaN으로 바꿔서 진행
                    # errors="ignore" : 변환을 하지 않는다.
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            all_data.append(df)
            print(f"{year_month}: {len(df)} 건 수집 완료")

        except Exception as e:
            print(f"{year_month} 에러 발생: {e}")
            continue


# API를 월/청크 단위로 여러 번 호출하면 all_data에 DataFrame이 여러 개 생긴다.
# DataFrame 합치기
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    # 인덱스는 버린다 -
    # 각 조각 DF의 인덱스가 모두 0부터 시작이라 중복 인덱스가 생깁니다. 새로 0,1,2…로 깨끗하게 재인덱싱해서 혼란이 되기 떄문에.
    print(f"전체 데이터 크기:", final_df.shape)
else:
    print("수집 데이터 없음")
    exit()


# 6. BigQuery 업로드
# 전체 테이블 경로
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# DataFrame -> BigQuery 테이블 업로드
job = client.load_table_from_dataframe(final_df, table_ref)
job.result() # 업로드가 끝날 때까지 대기

print("BigQuery 업로드 완료", table_ref)