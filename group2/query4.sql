CREATE TABLE capstone_t1_g2_q4
    (flight       STRING,
    avg_arr_delay DOUBLE)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "capstone_t1_g2_q4",
    "dynamodb.column.mapping"="flight:flight,avg_arrival_delay:avg_arrival_delay"
);

SET mapred.reduce.tasks=40;
SET hive.exec.reducers.max=40;
SET dynamodb.throughput.write.percent=1.0;
set hive.cli.print.header=true;

DROP TABLE capstone_t1_g2_q4;
CREATE TABLE capstone_t1_g2_q4
    (flight       STRING,
    avg_arr_delay DOUBLE);

INSERT OVERWRITE TABLE capstone_t1_g2_q4
SELECT
  concat(origin, "-", dest) AS flight,
  round(avg(ArrDelay), 2) AS avg_arr_delay
FROM aviation
WHERE cancelled = 0 AND ArrDelay IS NOT NULL
GROUP BY origin, dest

-- Query ID = hadoop_20170808202711_c74dbc04-f64c-4d44-a440-7f370fb69da7
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0021)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     33         33        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 33.73 s
-- ----------------------------------------------------------------------------------------------
-- Loading data to table default.capstone_t1_g2_q4
-- OK
-- flight	avg_arr_delay
-- Time taken: 34.599 seconds

SELECT * FROM capstone_t1_g2_q4 WHERE flight IN
  ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX')
ORDER BY flight, avg_arr_delay;

-- Query ID = hadoop_20170808202756_541492f0-affe-447d-bba2-3c518ba6965c
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0021)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 4.41 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g2_q4.flight	capstone_t1_g2_q4.avg_arr_delay
-- ATL-PHX	9.02
-- CMI-ORD	10.14
-- DFW-IAH	7.65
-- IND-CMH	2.9
-- JFK-LAX	6.64
-- LAX-SFO	9.59
-- Time taken: 5.011 seconds, Fetched: 6 row(s)
