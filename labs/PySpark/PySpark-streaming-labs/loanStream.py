from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("StructuredNetworkWordCountWindowed").getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
linesStr = spark.readStream.format('json').load('/mnt/c/Users/Miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/loan/loan.json')
# format('json').option('port', 9999).option('includeTimestamp', 'true').load()