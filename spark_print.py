from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", "172.25.0.12:9092,172.25.0.13:9092") \
.option("startingOffsets", "earliest") \
.option("subscribe", "AAPL") \
.load()

df1 = df.selectExpr("CAST(key AS STRING)", "CAST(CAST(value AS STRING) AS FLOAT)", "timestamp")

df1.show()