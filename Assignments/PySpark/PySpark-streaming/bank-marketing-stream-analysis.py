'''
	Create PySpark Application - bank-marketing-stream-analysis.py. Perform below operations.
	a) Consume Bank Marketing Campaign event from Kafka topic - bank-marketing-events
	b) Get AgeGroup wise SubscriptionCount
	c) Write the output into Kafka topic - bank-marketing-subcount
'''
#bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic bank-marketing-events

#bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bank-marketing-events < /mnt/c/Users/Miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 bank-marketing-stream-analysis.py 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.master('spark://MILES-BL-4819-LAP.:7077').appName("BANK Streaming Analysis").getOrCreate()


df = spark.readStream.format("kafka")\
	                .option("kafka.bootstrap.servers", "localhost:9092")\
                    .option("subscribe", "bank-marketing-events")\
	                .option("startingOffsets", "earliest")\
	                .load()

df.printSchema()

schema = StructType([ StructField("age", IntegerType(), True),
                      StructField("job", StringType(), True),
                      StructField("marital", StringType(), True),
                      StructField("education", StringType(), True),
                      StructField("default", StringType(), True),
                      StructField("balance", IntegerType(), True),
                      StructField("housing", StringType(), True),
                      StructField("loan", StringType(), True),
                      StructField("contact", IntegerType(), True),
                      StructField("day", IntegerType(), True),
                      StructField("month", StringType(), True),
                      StructField("duration", StringType(), True),
                      StructField("campaign", StringType(), True),
                      StructField("pdays", StringType(), True),
                      StructField("previous", StringType(), True),
                      StructField("poutcome", StringType(), True),
                      StructField("y", StringType(), True)])

newDf = df.select(from_csv(col("value").cast("String"), "val String").alias("status"))

newDf.printSchema()

newDf = newDf.select(split(newDf.status.val,'[;]',).alias("status"))

newDf = newDf.selectExpr("CAST(status AS STRING) AS key", "to_json(struct(*)) AS value")

# age_df = df.filter(col('y') == 'yes').select('age').withColumn('Age_group', when(col('age') <= 20 ,'Teenagers')\
#                                                                 .when((col('age') > 20) & (col('age') <= 40) , 'Youngsters')\
#                                                                 .when((col('age') > 40) & (col('age') <= 60) , 'MiddleAgers')\
#                                                                 .otherwise('Seniors')\
#                                                                 ).groupBy('Age_group').agg(count('age').alias('SubscriptionCount'))


query = newDf\
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("topic","bank-marketing-subcount")\
    .option("checkpointLocation", "bank-marketing-events") \
    .start().awaitTermination()