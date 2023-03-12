#!/bin/bash

echo "##### Started Execution #####"
alias spark-submit='/opt/spark/bin/spark-submit'
echo "executing bank-marketing-data-loading.py"
spark-submit bank-marketing-data-loading.py

if [ $? -eq 0 ]
then
    echo "executing Validations"
    spark-submit bank-marketing-validation.py
    if [ $? -eq 0 ]
    then 
        echo "executing  Transformations"
        spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2 bank-marketing-transformation.py
            if [ $? -eq 0 ]
            then
                echo "executing Export File"
                spark-submit --jars ~/mysql/mysql-connector-j-8.0.32.jar bank-marketing-export.py
                if [ $? -eq 0 ]
                    then
                        echo "Executed Export File"
                    else
                        echo "============== ERROR in Export File  ==================="
                fi
            else
                echo "============== ERROR in Transformation File  ==================="
            fi
    else
        echo "================ ERROR in Validation File  ===================="
    fi 
else
    echo "================ ERROR While Executing Loading File =================="
fi
echo "##### END ######"