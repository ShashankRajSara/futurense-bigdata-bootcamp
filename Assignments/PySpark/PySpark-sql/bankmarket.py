'''
	Bank Marketing Campaign Data Analysis with PySpark SQL

	Create PySpark Application - bank-marketing-analysis.py. Perform below operations.
	a) Load Bank Marketing Campaign Data from csv file
	b) Get AgeGroup wise SubscriptionCount
	c) Write the output in parquet file format
	d) Load the data from parquet file written above
	e) Show the data
	f) Filter AgeGroup with SubcriptionCount > 2000 and write into Avro file format
	g) Load the data from avro file written above
	h) Show the data

	1] spark-submit bank-marketing-analysis.py => Runs in local mode
	2] Run in cluster mode
	3] Schedule to PySparkApplication to run every N mins
''' 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.master('spark://MILES-BL-4819-LAP.:7077').appName('Bank Market').getOrCreate()

df=spark.read.format('csv')\
    .option('header','True')\
    .option('delimiter',';')\
    .load('/mnt/c/Users/Miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv')

dfAge = df.filter(col('y') == 'yes').groupBy('age').count().sort('count',ascending = False)

dfAge.write.mode("overwrite").parquet('/mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/bankParquet')
print("#######  Created Parquet File  #########")


dfParquet= spark.read.parquet('/mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/bankParquet')

print("##########   Parquet File Output    ########################")
print(dfParquet.show())


# f) Filter AgeGroup with SubcriptionCount > 2000 and write into Avro file format

age_cat_dict = {20:'Teen',40:'Young',60:'Middle',80:'Seniors',100:'Seniors'}
age_cat_udf = udf(lambda age: age_cat_dict[age],StringType())
df_age_cat = df.select('age','y').\
            filter(col('y') == 'yes').\
            withColumn('age_cat',age_cat_udf(ceil(df['age']/20)*20)).\
            groupBy('age_cat').count().filter(col('count') > 2000)

df_age_cat.write.mode("overwrite").format('avro').save('/mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/bankAvro')
print("#######  Created Avro File  #########")


dfAvro= spark.read.format('avro').load('/mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/bankAvro')

print("##########   Avro File Output    ########################")
print(dfAvro.show())


'''
	1] spark-submit bank-marketing-analysis.py => Runs in local mode

    spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2  bankmarket.py

	2] Run in cluster mode

    --Set Master = spark://MILES-BL-4819-LAP.:7077
    spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2  bankmarket.py

	3] Schedule to PySparkApplication to run every N mins

    

'''