--Customer Table
CREATE TABLE IF NOT EXISTS customers (cust_id int,
last_name String,
first_name String,
age int,
profession String)
COMMENT 'Customer Details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/mnt/c/Users/Miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/retail/customers.txt' OVERWRITE INTO TABLE customers;



CREATE TABLE IF NOT EXISTS transactions (trans_id int,
trans_date date,
cust_id int,
amount double,
category String,
desc String,
city String,
state String,
pymt_mode String)
COMMENT 'Transaction Details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/mnt/c/Users/Miles/Documents/GitHub/futurense_hadoop-pyspark/labs/dataset/retail/transactions.txt' OVERWRITE INTO TABLE transactions;

--Questions
--1.
SELECT c.cust_id, COUNT(trans_id) FROM customers c JOIN transactions t ON c.cust_id = t.cust_id GROUP BY c.cust_id;

--2.
SELECT c.cust_id, ROUND(SUM(amount),2) trans_amount FROM customers c JOIN transactions t ON c.cust_id = t.cust_id GROUP BY c.cust_id;

--3.
SELECT c.cust_id,first_name, ROUND(SUM(amount),2) trans_amount FROM customers c JOIN transactions t ON c.cust_id = t.cust_id GROUP BY c.cust_id,first_name ORDER BY trans_amount DESC LIMIT 3;

