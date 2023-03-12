'''
	Create PySpark Application - bank-marketing-validation.py. Perform below operations.
	a) Load Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/staging'
	b) Remove all 'unknown' job records 
	c) Replace 'unknown' contact nos with 1234567890 and 'unknown' poutcome with 'na'
	d) Write the output as Parquet format into HDFS file system under '/user/training/bankmarketing/validated'
	e) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/success' once the validation job completed successfully
	f) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/error' once the validation job is failed due to data error
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import subprocess
from datetime import datetime

spark = SparkSession.builder.master("local").appName("bankCleaningApp").getOrCreate()

#a
bank_df =  spark.read.parquet('hdfs://localhost:9000/user/training/bankmarketing/staging')

#b
newDf=bank_df.filter(col('job') != 'unknown')

#c
newDf= newDf.withColumn('contact',regexp_replace('contact','unknown','1234567890')).withColumn('poutcome',regexp_replace('poutcome','unknown','na'))


# create date
date = datetime.now().strftime("%Y-%m-%d")


try :

	#d
	#subprocess.run("hadoop fs -mkdir /user/training/bankmarketing/validated".split())
	
    newDf.write.mode('overwrite').format('parquet').save('hdfs://localhost:9000/user/training/bankmarketing/validated/')

   	#bank_df.write.parquet('hdfs://localhost:9000/user/training/bankmarketing/staging/')

	#e
    subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/".split())
    subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/success".split())

    subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/validated/ /user/training/bankmarketing/staging/{date}/success".split())


except Exception as e:
	print(e)

	#if there is any error in the file reading create a error directory
	subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/error".split())

	# move the data to error folder
	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/validated/ /user/training/bankmarketing/staging/{date}/error".split())

