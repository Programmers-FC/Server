from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
import sys

# ✅ Spark 환경 설정
conf = SparkConf()
conf.set("spark.driver.memory", "1g")
conf.set("spark.executor.memory", "1g")
conf.set("spark.driver.cores", "1")
conf.set("spark.executor.cores", "1")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# ✅ Spark 세션 생성
spark = SparkSession.builder \
    .appName("S3_CSV_to_Parquet") \
    .config(conf=conf) \
    .getOrCreate()

# ✅ 날짜 리스트 (2025-03-10 ~ 2025-03-14)
date_list = [f"2025-03-{str(i).zfill(2)}" for i in range(10, 15)]

# ✅ S3 버킷 정보
S3_BUCKET = "de5-finalproj-team2"

for DATE_STR in date_list:
    try:
        # ✅ S3 경로 설정
        S3_CSV_PATH = f"s3a://{S3_BUCKET}/crawl/{DATE_STR}/crawl_result_processed_{DATE_STR.replace('-', '')}.csv"
        S3_PARQUET_PATH = f"s3a://{S3_BUCKET}/analytics/ranking_info/{DATE_STR}/ranking_info_data.parquet/"

        # ✅ CSV 파일 로드
        df = spark.read.option("header", "true").csv(S3_CSV_PATH)

        # ✅ 컬럼명 매핑 (한글 → 영어)
        column_mapping = {
            "순위": "ranking",
            "등급": "division_id",
            "평점 점수": "rank_num",
            "감독명": "gamer_nickname",
            "레벨": "gamer_level",
            "팀 가치": "team_worth",
            "포인트": "points",
            "승률": "winning_rate",
            "총 승": "total_win",
            "총 무": "total_draw",
            "총 패": "total_lose",
            "포메이션": "formation",
        }

        # ✅ 컬럼명 변경 적용
        for kor, eng in column_mapping.items():
            if kor in df.columns:
                df = df.withColumnRenamed(kor, eng)

        # ✅ 불필요한 컬럼 삭제 (팀 이름)
        if "팀 이름" in df.columns:
            df = df.drop("팀 이름")

        # ✅ Parquet 파일 저장
        df.write.mode("overwrite").parquet(S3_PARQUET_PATH)

        print(f"✅ {DATE_STR} 데이터 변환 완료! 저장 경로: {S3_PARQUET_PATH}")

    except Exception as e:
        print(f"⚠️ {DATE_STR} 변환 중 오류 발생: {str(e)}")

print("🎉 모든 날짜의 데이터 변환 완료!")
