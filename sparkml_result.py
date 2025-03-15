from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import CountVectorizer, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from konlpy.tag import Okt
import re
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from pyspark import SparkConf
import sys
from pyspark.sql.functions import explode, col
from pyspark.sql.types import IntegerType,DateType

# 날짜 파라미터 받기

if len(sys.argv) != 2:
    print("Usage: spark-submit <script.py> <processing_date>")
    sys.exit(1)

processing_date = sys.argv[1]
print(f"Processing date: {processing_date}")

# Spark 환경 설정
conf = SparkConf()
conf.set("spark.driver.memory", "1g") # 드라이버 ram 용량 1GB
conf.set("spark.executor.memory", "1g") # Executor ram 용량 1GB
conf.set("spark.driver.cores","1") # driver CPU Core 사용량: 1개
conf.set("spark.executor.cores", "1") # Executor Core  사용량: 1개
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain") 

# SparkSession 생성
spark = SparkSession.builder.master("local[1]").appName('sparkml_result').config(conf=conf).getOrCreate()

#리뷰 데이터 s3로부터 가져옴
data_path = f"s3a://de5-finalproj-team2/raw_data/csv/review_data/{processing_date}/review_data.csv"
print(f"Reading csv file from: {data_path }")


data = spark.read.csv(f"{data_path}",header=True, sep=",", multiLine=True, quote='"', escape="\\", inferSchema=True)

#모델 불러오기 전까지 리뷰 데이터 전처리(순서대로 결측값 제거, 평점 제거, 불용어 제거 및 형태소 분석, 토큰화)

def remove_rating(text):
    return re.sub(r"평점\s?\d+\s?", "", text).strip()

# UDF(User Defined Function) 등록
remove_rating_udf = udf(remove_rating, StringType())

# Spark DataFrame에 적용
data = data.withColumn("comment", remove_rating_udf(data["comment"]))

stopwords = ['도', '는', '다', '의', '가', '이', '은', '한', '에', '하', '고','을', '를', '인', '듯', '과', '와', '네', '들', '듯', '지', '임', '게']

def clean_text(text):
    text = re.sub(r"[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", text)
    return text

    # OKT 형태소 분석을 활용한 토큰화 함수
def okt_tokenizer(text):
    if text is None:  # None 값 체크
        return []
    okt = Okt()  # UDF 내부에서 생성
    text = clean_text(text)
    tokens = okt.morphs(text)
    return [word for word in tokens if word not in stopwords]

tokenizer_udf = udf(okt_tokenizer, ArrayType(StringType()))

data = data.withColumn("tokens", tokenizer_udf(data["comment"]))


#모델 s3로부터 불러오고 분류 결과 도출

MODEL_PATH = "s3a://de5-finalproj-team2/model/spark_ml_pipeline"


model = PipelineModel.load(MODEL_PATH)

print(f"s3에서 모델 로드 완료: {MODEL_PATH}")


# 예측 실행
predictions = model.transform(data)
predictions=predictions.select("player_id","comment","prediction")


predictions = predictions.withColumnRenamed("player_id", "spid") \
                         .withColumnRenamed("comment", "review") \
                         .withColumn("spid", col("spid").cast(IntegerType())) \
                         .withColumn("review", col("review").cast(StringType())) \
                         .withColumn("prediction", col("prediction").cast(IntegerType()))

# ✅ 컬럼 순서 맞추기 (prediction이 맨 앞)
predictions = predictions.select("spid", "review", "prediction")


# Parquet 형식으로 저장
predictions.write.mode("overwrite").parquet(f's3a://de5-finalproj-team2/analytics/review_data_ML/{processing_date}')

print("예측 결과 저장 완료!")
