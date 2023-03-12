'''
	a) Load processed Bank Marketing Campaign Data in Avro format from HDFS file system under '/user/training/bankmarketing/processed'
	b) Export the data into RDBMS (MySQL DB) under bankmaketing schema and subcription_count table
	c) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/success' once the export job completed successfully
	d) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/error' once the export job is failed
'''

import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder.master('local').appName("bankmarketingExportApp").getOrCreate()

date =  datetime.now().strftime('%Y-%m-%d')

subprocess.run(f'hadoop fs -mkdir /user/training/bankmarketing/processed/{date}'.split())


try:
	avro_df = spark.read.format("avro").load("hdfs://localhost:9000/user/training/bankmarketing/processed/*.avro")

	avro_df.write.mode('append') \
	    .format("jdbc") \
	    .option("url", "jdbc:mysql://localhost/bankmarketing") \
	    .option("driver", "com.mysql.jdbc.Driver") \
	    .option("dbtable", "subscription_count") \
	    .option("user", "sqoop") \
	    .option("password", "sqoop") \
	    .save()
 
	avro_df.write.mode('overwrite').csv(path=f'hdfs://localhost:9000/user/training/bankmarketing/processed/{date}/success',header=True)

except Exception as e:
	print(e)

	subprocess.run(f'hadoop fs -mkdir /user/training/bankmarketing/processed/{date}/error'.split())

	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/processed/*.avro /user/training/bankmarketing/processed/{date}/error".split())
