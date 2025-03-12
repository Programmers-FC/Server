from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime
import sys

# Spark 환경 설정
conf = SparkConf()
conf.set("spark.driver.memory", "1g")
conf.set("spark.executor.memory", "1g")
conf.set("spark.driver.cores", "1")
conf.set("spark.executor.cores", "1")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
# Spark 세션 생셩
spark = SparkSession\
        .builder\
        .appName("S3_CSV_to_Parquet")\
        .getOrCreate()
DATE_STR = datetime.now().strftime('%Y-%m-%d')
# load to CSV
S3_BUCKET = "de5-finalproj-team2"
S3_CSV_PATH = f"s3a://{S3_BUCKET}/crawl/{DATE_STR}/crawl_result_processed_{DATE_STR.replace('-', '')}.csv"  # S3에서 가져올 CSV 파일
S3_PARQUET_PATH = f"s3a://{S3_BUCKET}/analytics/ranking_info/{DATE_STR}/ranking_info_data.parquet"  # 저장할 Parquet 경로


df = spark.read.option("header", "true").csv(S3_CSV_PATH)

# translate Korean into Eng
column_mapping = {
    "순위": "ranking",
    "등급": "division_id",
    "랭크번호": "rank_num",
    "감독명": "gamer_nickname",
    "레벨": "level",
    "팀 가치": "team_worth",
    "승점": "points",
    "승률": "winning_rate",
    "승": "total_win",
    "무": "total_draw",
    "패": "total_lose",
    "팀 이름": "team_name",
    "포메이션": "formation"
}

# 컬럼명 변경 적용
for kor, eng in column_mapping.items():
    if kor in df.columns:
        df = df.withColumnRenamed(kor, eng)

# Save to Parquet file
df.write.mode("overwrite").parquet(S3_PARQUET_PATH)

print("✅ Parquet 파일 변환 완료!")

