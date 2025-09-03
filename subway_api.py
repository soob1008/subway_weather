# 서울시 지하철 시간대 승하차 API 호출

import requests
import pandas as pd

API_KEY= "7078446565736f6f3130384c51784671"
year_month = "202506"

url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/CardSubwayTime/1/1000/{year_month}"

# API 호출
response = requests.get(url)
data = response.json()

# row 추출
rows = data['CardSubwayTime']['row']

df = pd.DataFrame(rows)


# 8. 결과 확인
print("✅ 데이터 컬럼:", df.columns.tolist())   # 컬럼명 출력
print("✅ 데이터 샘플 5개:")
print(df.head())   # 앞 5줄 미리보기

# CSV 파일로 저장
df.to_csv(f"subway_{year_month}.csv", index=False, encoding="utf-8-sig")
print(f"CSV 파일 저장 완료: subway_{year_month}.csv")