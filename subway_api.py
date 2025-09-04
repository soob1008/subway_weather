"""
서울시 지하철 시간대별 승하차 API 데이터를 수집하여
BigQuery 테이블에 업로드하는 예제 코드
"""

# ==============================
# 1. 라이브러리 불러오기
# ==============================
import requests           # API 호출용
import pandas as pd       # 데이터프레임 다루기
from google.cloud import bigquery   # BigQuery 연결용
from datetime import datetime

# ==============================
# 2. 기본 설정
# ==============================
API_KEY = "7078446565736f6f3130384c51784671"

PROJECT_ID = "subway-weather"
DATASET_ID = "subway_dataset"            # BigQuery에서 미리 만든 데이터셋 이름
TABLE_ID = "subway_usage"                # 테이블 이름 (없으면 자동 생성됨)

# BigQuery 클라이언트 생성 (환경변수 GOOGLE_APPLICATION_CREDENTIALS 설정 필요)
client = bigquery.Client(project=PROJECT_ID)

print("✅ BigQuery 연결 성공:", PROJECT_ID, client)


# ==============================
# 3. 수집할 기간 지정
# ==============================
# 예: 2024년 1월부터 2025년 9월까지
start_year, start_month = 2024, 1
end_year, end_month = 2025, 9

# ==============================
# 4. 데이터 수집
# ==============================
all_data = []  # 월별 데이터를 모아두는 리스트

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        # 마지막 월까지만 수집
        if (year == end_year) and (month > end_month):
            break

        year_month = f"{year}{month:02d}"  # 예: 202401
        print(f"📡 {year_month} 데이터 수집 중...")

        # API URL 구성
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/CardSubwayTime/1/1000/{year_month}"

        try:
            res = requests.get(url)
            data = res.json()  # JSON 응답 → Python dict 변환

            # 데이터 확인
            if "CardSubwayTime" not in data:
                print(f"❌ {year_month}: 데이터 없음")
                continue

            rows = data["CardSubwayTime"]["row"]
            df = pd.DataFrame(rows)

            # 연월 기록 추가
            df["USE_MON"] = year_month

            # 문자열 숫자 → 정수 변환
            for col in df.columns:
                if col.endswith("_RIDE_NUM") or col.endswith("_ALIGHT_NUM"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            all_data.append(df)
            print(f"✅ {year_month}: {len(df)}건 수집 완료")

        except Exception as e:
            print(f"⚠️ {year_month} 에러 발생: {e}")
            continue

# ==============================
# 5. DataFrame 합치기
# ==============================
if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    print("📊 전체 데이터 크기:", final_df.shape)
else:
    print("❌ 수집된 데이터 없음")
    exit()

# ==============================
# 6. BigQuery 업로드
# ==============================
# 테이블 전체 경로 (project.dataset.table)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# DataFrame → BigQuery 테이블 업로드
job = client.load_table_from_dataframe(final_df, table_ref)
job.result()  # 업로드가 끝날 때까지 대기

print("🎉 BigQuery 업로드 완료:", table_ref)