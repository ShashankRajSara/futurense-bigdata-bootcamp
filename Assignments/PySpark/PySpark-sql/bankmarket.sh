echo $(date -u)
echo "##### Started Executor #####"

alias spark-submit='/opt/spark/bin/spark-submit'
spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2  /mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/bankmarket.py >> /mnt/c/Users/Miles/Documents/GitHub/futurense-dataeng-bootcamp/Assignments/PySpark/PySpark-sql/log.txt

echo "##### Finished Executor #####"

echo "============================================"
echo "============================================"