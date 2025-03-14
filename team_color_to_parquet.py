from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import explode, split, regexp_extract, col, lit
from datetime import datetime
from pyspark.sql.types import StringType, IntegerType, DateType

# ✅ Spark 환경 설정
conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# ✅ Spark 세션 생성
spark = SparkSession.builder \
    .appName("Extract_Team_Color_Info") \
    .config(conf=conf) \
    .getOrCreate()

# ✅ 날짜 설정
DATE_STR = datetime.now().strftime('%Y-%m-%d')
DATE_STR_NODASH = DATE_STR.replace('-', '')

# ✅ S3 경로 설정
S3_BUCKET = "de5-finalproj-team2"
S3_CSV_PATH = f"s3a://{S3_BUCKET}/crawl/{DATE_STR}/crawl_result_processed_{DATE_STR_NODASH}.csv"
S3_PARQUET_PATH = f"s3a://{S3_BUCKET}/analytics/team_color_info/{DATE_STR}"

# ✅ CSV 파일 로드
df = spark.read.option("header", "true").csv(S3_CSV_PATH)

# ✅ 컬럼명 매핑 (한글 → 영어)
column_mapping = {
    "감독명": "gamer_nickname",
    "팀 이름": "team_color",
}

for kor, eng in column_mapping.items():
    if kor in df.columns:
        df = df.withColumnRenamed(kor, eng)

df = df.select("gamer_nickname", "team_color")

# ✅ 팀 컬러 여러 개일 경우 개별 컬럼으로 분리
df = df.withColumn("team_color", explode(split(df["team_color"], r'\|')))  # '|' 기준으로 분할

# ✅ 팀 컬러명과 인원수를 분리 (예: "첼시 (6명)" → "첼시", 6)
df = df.withColumn("team_count", regexp_extract(col("team_color"), r'\((\d+)', 1).cast(IntegerType()))  # 숫자 추출
df = df.withColumn("team_color", regexp_extract(col("team_color"), r'(.+?)\s*\(', 1))  # 팀명 추출

# ✅ 현재 날짜를 created_at으로 추가
df = df.withColumn("created_at", lit(datetime.now().strftime('%Y-%m-%d')).cast(DateType()))

# ✅ 컬럼 순서 맞추기 (Redshift 테이블 기준)
df = df.select("gamer_nickname", "team_color", "team_count", "created_at")

# ✅ Parquet 파일로 변환하여 S3에 저장
df.write.mode("overwrite").parquet(S3_PARQUET_PATH)

print(f"✅ Parquet 변환 완료! 저장 경로: {S3_PARQUET_PATH}")
