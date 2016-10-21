-- Create database if it does not yet exist
CREATE DATABASE IF NOT EXISTS abusser;

-- Use database
USE abusser;

-- Create CSV table
CREATE EXTERNAL TABLE prenoms_csv (
	name STRING,
	gender STRING,
	origin STRING,
	version DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '/user/abusser/hive/prenoms';

-- Fill table with file (will move file to table's location)
LOAD DATA INPATH '/user/abusser/hive/prenoms.csv' INTO TABLE prenoms_csv;

-- Create ORC table
CREATE EXTERNAL TABLE prenoms_orc (
	name STRING,
	gender ARRAY<STRING>,
	origin ARRAY<STRING>,
	version DOUBLE
)
STORED AS ORC
LOCATION '/user/abusser/hive/prenoms_orc';

-- Copy data from one table to another
INSERT INTO TABLE prenoms_orc SELECT 
	name,
	IF(gender="",array(),split(gender,",\\ ?")) AS gender,
	IF(origin="",array(),split(origin,",\\ ?")) AS origin,
	version
FROM prenoms_csv;

-- Q1: Find the number of first names for each origin.
SELECT origin, COUNT(name) AS name_count FROM (
	SELECT origin_exploded AS origin, name FROM prenoms_orc
	LATERAL VIEW explode(origin) temp AS origin_exploded
) AS temp GROUP BY origin;

-- Q2: Find the number of first names for each number of origins.
SELECT origin_count, COUNT(name) AS name_count FROM (
	SELECT size(origin) AS origin_count, name FROM prenoms_orc
) AS temp GROUP BY origin_count;

-- Q3: Find the proportion for each gender.
SELECT gender, gender_count / total_count * 100.0 AS gender_part FROM (
	SELECT gender_exploded AS gender, COUNT(gender_exploded) AS gender_count FROM prenoms_orc
	LATERAL VIEW explode(gender) temp AS gender_exploded
	GROUP BY gender_exploded
) AS temp1 JOIN (
	SELECT COUNT(*) AS total_count FROM prenoms_orc
) AS temp2;
