-- 1.	Load data and create a Hive table
CREATE TABLE IF NOT EXISTS bankmarket (
    age INT,
    job String,
    marital string,
    education string,
    default string,
    balance int,
    housing string,
    loan string,
    contact string,
    day int,
    month String,
    duration int,
    campaign int,
    pdays int,
    previous int,
    poutcome string,
    y string 
)
COMMENT 'Bank Campaign Data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/mnt/c/Users/miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv' OVERWRITE INTO TABLE bankmarket;


-- 2.	Give marketing success rate. (No. of people subscribed / total no. of entries)
SELECT SUM(IF(y='yes',1,0))/COUNT(*) SuccessRate FROM bankmarket;

-- 3.	Give marketing failure rate
SELECT 1-(SUM(IF(y='yes',1,0))/COUNT(*)) FailureRate FROM bankmarket;


-- 4.	Maximum, Mean, and Minimum age of the average targeted customer
SELECT MAX(age) Max_Age, ROUND(AVG(age),2) avg_age,MIN(age) min_age FROM bankmarket;

-- 5.	Check the quality of customers by checking the average balance, median balance of customers
SELECT AVG(balance) avg_balance,percentile(age, 0.5) median_balance FROM bankmarket;


-- 6.	Check if age matters in marketing subscription for deposit
SELECT age,SUM(IF(y='yes',1,0)) NoOfDeposits FROM bankmarket GROUP BY age LIMIT 5,20;

-- 7.	Check if marital status mattered for subscription to deposit.
SELECT marital,SUM(IF(y='yes',1,0)) NoOfDeposits FROM bankmarket GROUP BY marital;

-- 8.	Check if age and marital status together mattered for subscription to deposit scheme
SELECT age,marital,SUM(IF(y='yes',1,0)) NoOfDeposits FROM bankmarket GROUP BY age,marital ORDER BY NoOfDeposits DESC LIMIT 20;
