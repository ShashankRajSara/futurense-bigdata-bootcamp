'''
	a) Load validated Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/validated'
	b) Get AgeGroup wise SubscriptionCount
	c) Filter AgeGroup with SubcriptionCount > 2000 
	d) Write the output as Avro format into HDFS file system under '/user/training/bankmarketing/processed'
	e) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/success' once the trasnfomation job completed successfully
	f) Data should be moved to '/user/training/bankmarketing/validated/yyyymmdd/error' once the transformation job is failed
'''

#spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import subprocess
from datetime import datetime

spark = SparkSession.builder.master("spark://MILES-BL-4819-LAP.:7077").appName("Bank Data Transformation").getOrCreate()


cols="age INT,job String, marital String, education String, default String, balance DOUBLE , housing String,loan String ,contact INT, day INT, month String, duration INT, campaign INT, pdays INT, previous INT, poutcome String, y String"

#a
df =  spark.read.schema(cols).parquet('hdfs://localhost:9000/user/training/bankmarketing/validated/*.parquet')



age_df = df.filter(col('y') == 'yes').select('age').withColumn('Age_group', when(col('age') <= 20 ,'Teenagers')\
                                                                .when((col('age') > 20) & (col('age') <= 40) , 'Youngsters')\
                                                                .when((col('age') > 40) & (col('age') <= 60) , 'MiddleAgers')\
                                                                .otherwise('Seniors')\
                                                                ).groupBy('Age_group').agg(count('age').alias('SubscriptionCount'))

age_df=age_df.filter(col('SubscriptionCount')>2000)


date = datetime.now().strftime('%Y-%m-%d')
subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/validated/{date}".split())


try:
#d
    age_df.write.mode('overwrite').format('avro').save('hdfs://localhost:9000/user/training/bankmarketing/processed')

    age_df.write.mode('overwrite').csv(path= f"hdfs://localhost:9000/user/training/bankmarketing/validated/{date}/success",header=True)

except Exception as e:
    print(e)
    #if there is any error in the file reading create a error directory
    subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/validated/{date}/error".split())

    # move the data to error folder
    subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/validated/*.parquet /user/training/bankmarketing/validated/{date}/error".split())
