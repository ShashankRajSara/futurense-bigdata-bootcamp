CREATE TABLE IF NOT EXISTS movies (movieId int, title String, genre array<string>)
COMMENT 'Movie details'
PARTITIONED BY (YoR int) CLUSTERED BY (movieId) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

--LOAD Data 
LOAD DATA INPATH '/user/training/movies.csv' OVERWRITE INTO TABLE movies;


--Ratings
-- userId,movieId,rating,time_stamp
CREATE TABLE IF NOT EXISTS ratings (userId int, movieId String, rating float,time_stamp date)
COMMENT 'Ratings details'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count' = '1');

cd 
LOAD DATA INPATH '/user/training/ratings.csv' OVERWRITE INTO TABLE ratings;
--a
SELECT year(time_stamp), COUNT(movieId) movieCount FROM ratings r INNER JOIN movies m ON r.movieId = m.movieId 
GROUP BY year(time_stamp) ORDER BY movieCount DESC;

--b
SELECT year(time_stamp), title, genre FROM ratings r INNER JOIN movies m ON r.movieId = m.movieId 
ORDER BY rating DESC LIMIT 3;

--c
SELECT year(time_stamp), COUNT(genre) movieCount FROM ratings r INNER JOIN movies m ON r.movieId = m.movieId 
GROUP BY year(time_stamp) ;

--d
SELECT * FROM movies TABLESAMPLE(BUCKET 2 OUT OF 3);