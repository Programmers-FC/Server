from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lit
from pyspark.sql.types import IntegerType, LongType, DecimalType, StringType, DateType
from datetime import datetime

# ✅ Spark 세션 생성
spark = SparkSession.builder \
    .appName("Daily_Data_Collection") \
    .getOrCreate()

# ✅ S3 경로 설정
S3_BUCKET = "de5-finalproj-team2"

# ✅ 오늘 날짜 가져오기
DATE_STR = datetime.now().strftime('%Y-%m-%d')
DATE_STR_NODASH = DATE_STR.replace('-', '')

S3_CSV_PATH = f"s3a://{S3_BUCKET}/crawl/{DATE_STR}/crawl_result_processed_{DATE_STR_NODASH}.csv"
S3_PARQUET_PATH = f"s3a://{S3_BUCKET}/analytics/ranking_info/{DATE_STR}"

print(f"📂 Processing: {S3_CSV_PATH} → {S3_PARQUET_PATH}")

# ✅ CSV 파일 로드
df = spark.read.option("header", "true").csv(S3_CSV_PATH)

# ✅ 컬럼명 매핑
column_mapping = {
    "순위": "ranking",
    "등급": "division_id",
    "랭크 번호": "rank_num",
    "감독명": "gamer_nickname",
    "레벨": "gamer_level",
    "팀 가치": "team_worth",
    "승점": "points",
    "승률": "winning_rate",
    "승": "total_win",
    "무": "total_draw",
    "패": "total_lose",
    "팀 이름": "team_name",  # ✅ 추가 (필요 시 삭제)
    "포메이션": "formation"
}

# ✅ 컬럼명 변경
for kor, eng in column_mapping.items():
    matched_columns = [c for c in df.columns if c.strip() == kor]
    if matched_columns:
        df = df.withColumnRenamed(matched_columns[0], eng)

# ✅ "팀 이름" 컬럼 삭제
if "team_name" in df.columns:
    df = df.drop("team_name")  

# ✅ 승률에서 '%' 제거 후 실수로 변환
if "winning_rate" in df.columns:
    df = df.withColumn("winning_rate", regexp_replace(col("winning_rate"), "%", "").cast(DecimalType(10, 2)))

# ✅ 데이터 타입 변환 (Redshift 테이블과 일치하도록)
df = df.withColumn("ranking", col("ranking").cast(IntegerType()))
df = df.withColumn("division_id", col("division_id").cast(IntegerType()))
df = df.withColumn("gamer_nickname", col("gamer_nickname").cast(StringType()))
df = df.withColumn("gamer_level", col("gamer_level").cast(IntegerType()))
df = df.withColumn("team_worth", col("team_worth").cast(LongType()))
df = df.withColumn("points", col("points").cast(DecimalType(10, 2)))
df = df.withColumn("winning_rate", col("winning_rate").cast(DecimalType(10, 2)))
df = df.withColumn("formation", col("formation").cast(StringType()))
df = df.withColumn("total_win", col("total_win").cast(IntegerType()))
df = df.withColumn("total_draw", col("total_draw").cast(IntegerType()))
df = df.withColumn("total_lose", col("total_lose").cast(IntegerType()))

# ✅ created_at 컬럼 추가
df = df.withColumn("created_at", lit(DATE_STR).cast(DateType()))

# ✅ 컬럼 순서 정리 (Redshift 테이블 기준)
df = df.select(
    "gamer_nickname", "division_id", "gamer_level", "team_worth",
    "points", "winning_rate", "total_win", "total_draw", "total_lose",
    "formation", "created_at", "ranking"
)

# ✅ Parquet 저장 (덮어쓰기 모드)
df.write.mode("overwrite").parquet(S3_PARQUET_PATH)

print(f"✅ {DATE_STR} 데이터 변환 완료: {S3_PARQUET_PATH}")
