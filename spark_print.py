from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

df = spark \
.read \
.format("kafka") \
.option("kafka.bootstrap.servers", "172.25.0.12:9092,172.25.0.13:9092") \
.option("startingOffsets", "earliest") \
.option("subscribe", "a5_acc") \
.load()

df1 = df.selectExpr("CAST(key AS STRING)", "CAST(CAST(value AS STRING) AS FLOAT)", "timestamp")

df1.show()

#SQL table

df1.createOrReplaceTempView("apple_stockprice")

highest = spark.sql("SELECT key as time, value as highest_price_day FROM apple_stockprice WHERE value IN (select max(value) FROM apple_stockprice)")
highest.show()

lowest = spark.sql("SELECT key as time, value as lowest_price_day FROM apple_stockprice WHERE value IN (select min(value) FROM apple_stockprice)")
lowest.show()

standard = spark.sql("SELECT stddev(value) as standard_deviation_day FROM apple_stockprice")
standard.show()
