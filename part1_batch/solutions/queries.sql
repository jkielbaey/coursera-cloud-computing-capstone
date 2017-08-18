--  Optimizations

SET hive.cli.print.header=true;

SET hive.exec.parallel=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
ANALYZE TABLE aviation PARTITION (Yeard, Monthd) COMPUTE STATISTICS;
ANALYZE TABLE aviation PARTITION (Yeard, Monthd) COMPUTE STATISTICS FOR COLUMNS;

-- Hive & DynamoDB Optimizations.
SET dynamodb.throughput.read.percent=1.0;
SET dynamodb.throughput.write.percent=1.0;
SET dynamodb.max.map.tasks=50;
SET mapred.reduce.tasks=50;
SET hive.exec.reducers.max=50;

--  Group 1

--
-- Question 1.1
--
SELECT
  o.origin AS airport,
  o.total_departing_flights + d.total_arriving_flights AS total_flights
FROM
  (SELECT origin, count(flightnum) AS total_departing_flights FROM aviation GROUP BY origin) o
  JOIN
  (SELECT dest, count(flightnum) AS total_arriving_flights FROM aviation GROUP BY dest) d
  ON o.origin = d.dest
ORDER BY total_flights DESC
LIMIT 10;

-- Query ID = hadoop_20170808183504_42b7eab3-8aec-451d-90d9-8aac4452c633
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0012)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     31         31        0        0       0       0
-- Map 5 .......... container     SUCCEEDED     31         31        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0
-- Reducer 6 ...... container     SUCCEEDED     40         40        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 06/06  [==========================>>] 100%  ELAPSED TIME: 57.13 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- ORD  12449354
-- ATL  11540422
-- DFW  10799303
-- LAX  7723596
-- PHX  6585534
-- DEN  6273787
-- DTW  5636622
-- IAH  5480734
-- MSP  5199213
-- SFO  5171023
-- Time taken: 62.204 seconds, Fetched: 10 row(s)

--
-- Question 1.2
--
SELECT
  uniquecarrier AS airline,
  round(avg(ArrDelay),2) as avg_delay
FROM aviation
WHERE cancelled = 0
GROUP BY uniquecarrier
ORDER BY avg_delay
LIMIT 10;

-- Query ID = hadoop_20170808185708_aba44552-292c-4bba-ad5d-05f04fdf2bf5
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0016)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     33         33        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 29.00 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- HA -1.01
-- AQ 1.16
-- PS 1.45
-- ML (1) 4.75
-- PA (1) 5.32
-- F9 5.47
-- WN 5.56
-- NW 5.56
-- OO 5.74
-- 9E 5.87
-- Time taken: 30.122 seconds, Fetched: 10 row(s)

--
-- Question 1.3
--
SELECT
  DayOfWeek AS Weekday,
  round(avg(ArrDelay),2) as avg_delay
FROM aviation
WHERE cancelled = 0
GROUP BY DayOfWeek
ORDER BY avg_delay;

-- Query ID = hadoop_20170808185910_58c60324-1b01-4def-a899-b5d91c639bf9
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0016)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     33         33        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 29.01 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- 6  4.3
-- 2  5.99
-- 7  6.61
-- 1  6.72
-- 3  7.2
-- 4  9.09
-- 5  9.72
-- Time taken: 29.936 seconds, Fetched: 7 row(s)


--  Group 2

--
-- Question 2.1
--
-- aws dynamodb create-table --table-name Capstone_2_1 --attribute-definitions AttributeName=Airport,AttributeType=S AttributeName=Rank,AttributeType=N --key-schema AttributeName=Airport,KeyType=HASH AttributeName=Rank,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 --output text
DROP TABLE capstone_2_1;
CREATE EXTERNAL TABLE capstone_2_1
    (airport   STRING,
    airline    STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "Capstone_2_1",
    "dynamodb.column.mapping"="airport:Airport,airline:Airline,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE capstone_2_1
SELECT *
FROM (
  SELECT
    *,
    rank() over (PARTITION BY airport ORDER BY avg_delay, airline) rnk
  FROM (
    SELECT
      origin AS airport,
      uniquecarrier AS airline,
      round(avg(DepDelay),2) as avg_delay
    FROM aviation
    WHERE cancelled = 0
    GROUP BY origin, uniquecarrier
  ) unranked
) ranked
WHERE ranked.rnk <= 10;

-- Query ID = hadoop_20170808193128_8d809384-8e64-41c7-867d-225c73954c7f
-- Total jobs = 1
-- Launching Job 1 out of 1
-- Tez session was closed. Reopening...
-- Session re-established.
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0019)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     33         33        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     10         10        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 33.03 s
-- ----------------------------------------------------------------------------------------------
-- Loading data to table default.capstone_t1_g2_q1
-- OK
-- _col0  _col1 _col2 _col3
-- Time taken: 38.618 seconds

SELECT * FROM capstone_2_1 WHERE airport IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO');

-- Query ID = hadoop_20170808193358_81d8494f-246d-4891-826c-1e74ceab4ce2
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0019)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 3.99 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g2_q1.airport  capstone_t1_g2_q1.airline capstone_t1_g2_q1.avg_dep_delay capstone_t1_g2_q1.rank
-- BWI  F9  0.76  1
-- BWI  PA (1)  4.76  2
-- BWI  CO  5.18  3
-- BWI  YV  5.5 4
-- BWI  NW  5.71  5
-- BWI  AA  6.0 6
-- BWI  9E  7.24  7
-- BWI  US  7.49  8
-- BWI  DL  7.68  9
-- BWI  UA  7.74  10
-- CMI  OH  0.61  1
-- CMI  US  2.03  2
-- CMI  TW  4.12  3
-- CMI  PI  4.46  4
-- CMI  DH  6.03  5
-- CMI  EV  6.67  6
-- CMI  MQ  8.02  7
-- IAH  NW  3.56  1
-- IAH  PA (1)  3.98  2
-- IAH  PI  3.99  3
-- IAH  US  5.06  4
-- IAH  F9  5.55  5
-- IAH  AA  5.7 6
-- IAH  TW  6.05  7
-- IAH  WN  6.23  8
-- IAH  OO  6.59  9
-- IAH  MQ  6.71  10
-- LAX  MQ  2.41  1
-- LAX  OO  4.22  2
-- LAX  FL  4.73  3
-- LAX  TZ  4.76  4
-- LAX  PS  4.86  5
-- LAX  NW  5.12  6
-- LAX  F9  5.73  7
-- LAX  HA  5.81  8
-- LAX  YV  6.02  9
-- LAX  US  6.75  10
-- MIA  9E  -3.0  1
-- MIA  EV  1.2 2
-- MIA  TZ  1.78  3
-- MIA  XE  1.87  4
-- MIA  PA (1)  4.2 5
-- MIA  NW  4.5 6
-- MIA  US  6.09  7
-- MIA  UA  6.87  8
-- MIA  ML (1)  7.5 9
-- MIA  FL  8.57  10
-- SFO  TZ  3.95  1
-- SFO  MQ  4.85  2
-- SFO  F9  5.16  3
-- SFO  PA (1)  5.29  4
-- SFO  NW  5.76  5
-- SFO  PS  6.3 6
-- SFO  DL  6.56  7
-- SFO  CO  7.08  8
-- SFO  US  7.53  9
-- SFO  TW  7.79  10
-- Time taken: 4.579 seconds, Fetched: 57 row(s)


--
-- Question 2.2
--
-- aws dynamodb create-table --table-name Capstone_2_2 --attribute-definitions AttributeName=Airport,AttributeType=S AttributeName=Rank,AttributeType=N --key-schema AttributeName=Airport,KeyType=HASH AttributeName=Rank,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 --output text
DROP TABLE capstone_2_2;
CREATE EXTERNAL TABLE capstone_2_2
    (origin    STRING,
    dest       STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "Capstone_2_2",
    "dynamodb.column.mapping"="origin:Airport,dest:Destination,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE capstone_2_2
SELECT *
FROM (
  SELECT
    *,
    rank() over (PARTITION BY origin ORDER BY avg_delay, dest) rnk
  FROM (
    SELECT
      origin AS origin,
      dest AS dest,
      round(avg(DepDelay),2) as avg_delay
    FROM aviation
    WHERE cancelled = 0
    GROUP BY origin, dest
  ) unranked
) ranked
WHERE ranked.rnk <= 10;

-- Query ID = hadoop_20170808194912_6d2219d6-9b30-467f-b15e-1cbdef30f33a
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0020)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     33         33        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     10         10        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 35.17 s
-- ----------------------------------------------------------------------------------------------
-- Loading data to table default.capstone_t1_g2_q2
-- OK
-- _col0  _col1 _col2 _col3
-- Time taken: 36.006 seconds

SELECT * FROM capstone_2_2 WHERE origin IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO');

-- Query ID = hadoop_20170808195017_0f13e097-117d-4c44-836a-a25944511e51
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0020)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 5.04 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g2_q2.origin capstone_t1_g2_q2.destination capstone_t1_g2_q2.avg_dep_delay capstone_t1_g2_q2.rank
-- BWI  SAV -7.0  1
-- BWI  MLB 1.16  2
-- BWI  DAB 1.47  3
-- BWI  SRQ 1.59  4
-- BWI  IAD 1.79  5
-- BWI  UCA 3.65  6
-- BWI  CHO 3.74  7
-- BWI  GSP 4.2 8
-- BWI  SJU 4.44  9
-- BWI  OAJ 4.47  10
-- CMI  ABI -7.0  1
-- CMI  PIT 1.1 2
-- CMI  CVG 1.89  3
-- CMI  DAY 3.12  4
-- CMI  STL 3.98  5
-- CMI  PIA 4.59  6
-- CMI  DFW 5.94  7
-- CMI  ATL 6.67  8
-- CMI  ORD 8.19  9
-- IAH  MSN -2.0  1
-- IAH  AGS -0.62 2
-- IAH  MLI -0.5  3
-- IAH  EFD 1.89  4
-- IAH  HOU 2.17  5
-- IAH  JAC 2.57  6
-- IAH  MTJ 2.95  7
-- IAH  RNO 3.22  8
-- IAH  BPT 3.6 9
-- IAH  VCT 3.61  10
-- LAX  SDF -16.0 1
-- LAX  IDA -7.0  2
-- LAX  DRO -6.0  3
-- LAX  RSW -3.0  4
-- LAX  LAX -2.0  5
-- LAX  BZN -0.73 6
-- LAX  MAF 0.0 7
-- LAX  PIH 0.0 8
-- LAX  IYK 1.27  9
-- LAX  MFE 1.38  10
-- MIA  SHV 0.0 1
-- MIA  BUF 1.0 2
-- MIA  SAN 1.71  3
-- MIA  SLC 2.54  4
-- MIA  HOU 2.91  5
-- MIA  ISP 3.65  6
-- MIA  MEM 3.75  7
-- MIA  PSE 3.98  8
-- MIA  TLH 4.26  9
-- MIA  MCI 4.61  10
-- SFO  SDF -10.0 1
-- SFO  MSO -4.0  2
-- SFO  PIH -3.0  3
-- SFO  LGA -1.76 4
-- SFO  PIE -1.34 5
-- SFO  OAK -0.81 6
-- SFO  FAR 0.0 7
-- SFO  BNA 2.43  8
-- SFO  MEM 3.3 9
-- SFO  SCK 4.0 10
-- Time taken: 5.912 seconds, Fetched: 59 row(s)


--
-- Question 2.3
--
-- aws dynamodb create-table --table-name Capstone_2_3 --attribute-definitions AttributeName=Flight,AttributeType=S AttributeName=Rank,AttributeType=N --key-schema AttributeName=Flight,KeyType=HASH AttributeName=Rank,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 --output text
DROP TABLE capstone_2_3;
CREATE EXTERNAL TABLE capstone_2_3
    (flight    STRING,
    airline    STRING,
    avg_delay  DOUBLE,
    rank       BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "Capstone_2_3",
    "dynamodb.column.mapping"="flight:Flight,airline:Airline,avg_delay:AvgDelay,rank:Rank"
);

INSERT OVERWRITE TABLE capstone_2_3
SELECT
  concat(ranked.x, "-", ranked.y) AS flight,
  ranked.airline,
  ranked.avg_delay,
  ranked.rnk
FROM (
  SELECT
    *,
    rank() over (PARTITION BY x, y ORDER BY avg_delay, airline) rnk
  FROM (
    SELECT
      origin AS x,
      dest AS y,
      uniquecarrier AS airline,
      round(avg(ArrDelay), 2) AS avg_delay
    FROM aviation
    WHERE cancelled = 0 AND ArrDelay IS NOT NULL
    GROUP BY origin, dest, uniquecarrier
  ) unranked
) ranked
WHERE ranked.rnk <= 10;

-- Query ID = hadoop_20170808201912_8a41d64d-0c1c-4bf1-9cef-55f5f7e738e0
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
-- Reducer 3 ...... container     SUCCEEDED     10         10        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 36.40 s
-- ----------------------------------------------------------------------------------------------
-- Loading data to table default.capstone_t1_g2_q3
-- OK
-- _col0  _col1 _col2 _col3
-- Time taken: 37.309 seconds

SELECT * FROM capstone_2_3 WHERE flight IN ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX');

-- Query ID = hadoop_20170808202014_700a275c-09c4-4d83-854b-2f7bbd8de6fc
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
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 4.28 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g2_q3.flight capstone_t1_g2_q3.airline capstone_t1_g2_q3.avg_arr_delay capstone_t1_g2_q3.rank
-- ATL-PHX  FL  4.55  1
-- ATL-PHX  US  6.29  2
-- ATL-PHX  HP  8.48  3
-- ATL-PHX  EA  8.95  4
-- ATL-PHX  DL  9.81  5
-- CMI-ORD  MQ  10.14 1
-- DFW-IAH  PA (1)  -1.6  1
-- DFW-IAH  EV  5.09  2
-- DFW-IAH  UA  5.41  3
-- DFW-IAH  CO  6.49  4
-- DFW-IAH  OO  7.56  5
-- DFW-IAH  XE  8.09  6
-- DFW-IAH  AA  8.38  7
-- DFW-IAH  DL  8.6 8
-- DFW-IAH  MQ  9.1 9
-- IND-CMH  CO  -2.55 1
-- IND-CMH  AA  5.5 2
-- IND-CMH  HP  5.7 3
-- IND-CMH  NW  5.76  4
-- IND-CMH  US  6.88  5
-- IND-CMH  DL  10.69 6
-- IND-CMH  EA  10.81 7
-- JFK-LAX  UA  3.31  1
-- JFK-LAX  HP  6.68  2
-- JFK-LAX  AA  6.9 3
-- JFK-LAX  DL  7.93  4
-- JFK-LAX  PA (1)  11.02 5
-- JFK-LAX  TW  11.7  6
-- LAX-SFO  TZ  -7.62 1
-- LAX-SFO  PS  -2.15 2
-- LAX-SFO  F9  -2.03 3
-- LAX-SFO  EV  6.96  4
-- LAX-SFO  AA  7.39  5
-- LAX-SFO  MQ  7.81  6
-- LAX-SFO  US  7.96  7
-- LAX-SFO  WN  8.79  8
-- LAX-SFO  CO  9.35  9
-- LAX-SFO  NW  9.85  10
-- Time taken: 4.846 seconds, Fetched: 38 row(s)


--
-- Question 2.4
--
-- aws dynamodb create-table --table-name Capstone_2_4 --attribute-definitions AttributeName=Flight,AttributeType=S --key-schema AttributeName=Flight,KeyType=HASH --provisioned-throughput ReadCapacityUnits=50,WriteCapacityUnits=50 --output text
DROP TABLE capstone_2_4;
CREATE EXTERNAL TABLE capstone_2_4
    (flight    STRING,
    avg_delay  DOUBLE)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "Capstone_2_4",
    "dynamodb.column.mapping"="flight:Flight,avg_delay:AvgDelay"
);

INSERT OVERWRITE TABLE capstone_2_4
SELECT
  concat(origin, "-", dest) AS flight,
  round(avg(ArrDelay), 2) AS avg_delay
FROM aviation
WHERE cancelled = 0 AND ArrDelay IS NOT NULL
GROUP BY origin, dest;

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
-- flight avg_arr_delay
-- Time taken: 34.599 seconds

SELECT * FROM capstone_2_4 WHERE flight IN ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX');

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
-- capstone_t1_g2_q4.flight capstone_t1_g2_q4.avg_arr_delay
-- ATL-PHX  9.02
-- CMI-ORD  10.14
-- DFW-IAH  7.65
-- IND-CMH  2.9
-- JFK-LAX  6.64
-- LAX-SFO  9.59
-- Time taken: 5.011 seconds, Fetched: 6 row(s)


-- Group 3

--
--  Question 3.1
--
SELECT
  o.origin AS airport,
  o.total_departing_flights + d.total_arriving_flights AS total_flights,
  rank() OVER (ORDER BY o.total_departing_flights + d.total_arriving_flights DESC) rnk
FROM
  (SELECT origin, count(flightnum) AS total_departing_flights FROM aviation WHERE cancelled = 0 GROUP BY origin) o
  JOIN
  (SELECT dest, count(flightnum) AS total_arriving_flights FROM aviation WHERE cancelled = 0  GROUP BY dest) d
  ON o.origin = d.dest
ORDER BY rnk;

-- airport  total_flights
-- ORD  12051796
-- ATL  11323515
-- DFW  10591818
-- LAX  7586304
-- PHX  6505078
-- DEN  6183518
-- DTW  5504120
-- IAH  5416653
-- MSP  5087036
-- SFO  5062339
-- STL  5031131
-- EWR  4979913
-- LAS  4917971
-- CLT  4735669
-- LGA  4179969
-- BOS  4160565
-- PHL  3974631
-- PIT  3855898
-- SLC  3772753
-- SEA  3684674
-- MCO  3682356
-- CVG  3608316
-- DCA  3356098
-- BWI  3182257
-- SAN  2873824
-- MIA  2693441
-- CLE  2661971
-- IAD  2485562
-- JFK  2459476
-- TPA  2458402
-- HOU  2237673
-- MEM  2233284
-- BNA  2198751
-- MDW  2186580
-- MCI  2179902
-- OAK  2163750
-- SJC  2053084
-- PDX  1966609
-- RDU  1912261
-- FLL  1883718
-- MSY  1771517
-- DAL  1766130
-- IND  1536168
-- SMF  1519532
-- SNA  1519381
-- SAT  1507565
-- AUS  1491855
-- ONT  1447980
-- ABQ  1418398
-- CMH  1403596
-- BDL  1198809
-- BUR  1080161
-- PBI  994578
-- HNL  966774
-- JAX  965722
-- ELP  962455
-- RNO  953073
-- BUF  904984
-- OKC  897801
-- TUL  873128
-- SDF  860266
-- SJU  857773
-- MKE  823655
-- PVD  813629
-- ORF  797473
-- TUS  774898
-- BHM  756388
-- OMA  755955
-- RSW  748028
-- ANC  723735
-- GSO  718488
-- DAY  706566
-- ROC  681842
-- RIC  659624
-- SYR  621397
-- LIT  612897
-- ALB  540997
-- COS  501871
-- GEG  491093
-- BOI  445902
-- GRR  445063
-- MHT  443264
-- CHS  413711
-- DSM  409849
-- ICT  389164
-- TYS  373684
-- GSP  371283
-- JAN  354103
-- SAV  344996
-- LBB  321730
-- SRQ  315859
-- CAE  313763
-- OGG  311357
-- MDT  309256
-- PWM  296579
-- HSV  296190
-- LGB  293353
-- PNS  292104
-- MAF  291443
-- ISP  287719
-- BTR  280035
-- PSP  275075
-- MSN  272151
-- FAT  256087
-- LEX  244263
-- AMA  244001
-- SHV  239423
-- CID  235627
-- ABE  229485
-- CRP  228315
-- SBA  223952
-- MOB  223889
-- HPN  223365
-- HRL  215710
-- BTV  205128
-- FSD  191366
-- TLH  191015
-- FAI  183175
-- BIL  172503
-- JNU  164060
-- DAB  163755
-- XNA  160574
-- GRB  158424
-- MYR  155070
-- SGF  152302
-- CAK  151170
-- KOA  146726
-- FWA  144306
-- LIH  140824
-- MLI  140686
-- MBS  139028
-- EUG  136926
-- BZN  133485
-- CHA  132133
-- ROA  129296
-- MFE  128596
-- MRY  126334
-- MLB  125795
-- LAN  124545
-- SBN  122230
-- FAR  121036
-- LNK  120271
-- AZO  117876
-- GTF  113786
-- CRW  113770
-- STT  110957
-- RST  108985
-- TOL  108849
-- SWF  107797
-- ILM  106882
-- AVL  106538
-- VPS  106146
-- FNT  105201
-- MSO  104823
-- MFR  103718
-- MGM  100410
-- RAP  100213
-- AGS  99989
-- BGR  99929
-- MLU  96581
-- KTN  95453
-- GPT  91408
-- BIS  90703
-- TRI  90476
-- EVV  88242
-- AVP  87006
-- JAC  85309
-- FAY  82928
-- PHF  82830
-- PIA  79812
-- SBP  77261
-- PSC  72808
-- LFT  71781
-- BFL  69871
-- ITO  68011
-- IDA  67942
-- GNV  67692
-- GJT  65486
-- TVC  63466
-- FCA  62256
-- DLH  60703
-- ERI  54035
-- SIT  52861
-- LSE  52382
-- DET  51251
-- MOT  51217
-- STX  50646
-- CMI  50293
-- CPR  49733
-- HLN  49726
-- BGM  49475
-- PFN  48818
-- ATW  45353
-- EGE  45336
-- ELM  44741
-- GFK  44250
-- ACV  44041
-- YUM  43191
-- BMI  41736
-- GRK  41567
-- RDM  38981
-- OME  38856
-- ABI  38414
-- OTZ  37561
-- ACT  37255
-- SGU  36907
-- DRO  36755
-- CHO  36605
-- TYR  36285
-- CLL  35949
-- HDN  35851
-- BET  35356
-- CSG  34714
-- SUN  34121
-- ITH  34024
-- SUX  33689
-- AEX  32939
-- FSM  32714
-- LRD  32488
-- HTS  32421
-- MTJ  32401
-- LAW  31725
-- FLG  31713
-- SPS  30994
-- EYW  30888
-- GUM  29480
-- SJT  29423
-- BRO  28104
-- CDV  27988
-- YAK  27583
-- CLD  27179
-- WRG  27045
-- PSG  26930
-- ORH  25302
-- ASE  24955
-- EKO  24193
-- TWF  23822
-- BRW  23518
-- RDD  22645
-- ILE  22378
-- MOD  22334
-- SMX  22309
-- PIH  21021
-- SCC  20913
-- TXK  20725
-- BTM  20648
-- OAJ  20513
-- DHN  19983
-- OXR  18156
-- ADQ  18027
-- DBQ  17704
-- SPN  17458
-- LYH  17385
-- GGG  16976
-- ACY  16912
-- BPT  16421
-- BQN  16199
-- LCH  15348
-- ABY  15186
-- GTR  14328
-- GUC  14215
-- MCN  13895
-- VLD  13894
-- CIC  13778
-- HVN  13576
-- BLI  13265
-- CWA  13261
-- BQK  13251
-- COD  13014
-- PUB  12655
-- SCE  12533
-- PIE  12455
-- MEI  11868
-- CEC  11856
-- IYK  11516
-- AKN  10989
-- IPL  10986
-- GCN  10871
-- ISO  10832
-- SPI  10708
-- MQT  10371
-- FLO  9549
-- DUT  9237
-- DLG  8916
-- UCA  8663
-- CCR  7953
-- APF  7286
-- CDC  7250
-- EFD  6956
-- PMD  6472
-- SCK  6326
-- TVL  5740
-- PSE  5471
-- LWS  5259
-- VCT  4728
-- ROR  4238
-- TUP  3763
-- VIS  3692
-- ROP  3658
-- EAU  3563
-- GST  3504
-- HHH  3391
-- YKM  3339
-- LWB  3327
-- ACK  3292
-- TTN  3220
-- WYS  3191
-- RFD  3104
-- EWN  2513
-- ALO  2100
-- ROW  2031
-- SLE  1759
-- PLN  1498
-- ILG  1497
-- GCC  1478
-- HKY  1286
-- YAP  1284
-- RKS  1166
-- TEX  1144
-- ADK  1056
-- CMX  975
-- ANI  855
-- KSM  822
-- MKG  782
-- RHI  731
-- SOP  618
-- LNY  562
-- MKK  542
-- OTH  540
-- LMT  536
-- INL  536
-- BJI  376
-- MTH  252
-- MAZ  224
-- MIB  176
-- RDR  92
-- OGD  25
-- CYS  14
-- CKB  14
-- PVU  12
-- PIR  8
-- FMN  8
-- GLH  4
-- BFF  3
-- MKC  3


--
--  Question 3.2
--
DROP TABLE capstone_3_2;
CREATE TABLE capstone_3_2 (
  x STRING,
  y STRING,
  airline1 STRING,
  flightnum1 STRING,
  flightdate1 STRING,
  departure1 STRING,
  delay1 DOUBLE,
  z STRING,
  airline2 STRING,
  flightnum2 STRING,
  flightdate2 STRING,
  departure2 STRING,
  delay2 DOUBLE,
  total_delay DOUBLE,
  itinerary STRING);

INSERT OVERWRITE TABLE capstone_3_2
SELECT
  leg1.origin AS x,
  leg1.dest AS y,
  leg1.uniquecarrier AS airline1,
  leg1.flightnum AS flight_num1,
  leg1.flightdate AS flight_date1,
  leg1.DepTime AS departure1,
  leg1.ArrDelay AS delay1,
  leg2.dest AS z,
  leg2.uniquecarrier AS airline2,
  leg2.flightnum AS flight_num2,
  leg2.flightdate AS flight_date2,
  leg2.DepTime AS departure2,
  leg2.ArrDelay AS delay2,
  (leg1.ArrDelay + leg2.ArrDelay) AS total_delay,
  concat(leg1.origin, "-", leg1.dest, "-", leg2.dest) AS itinerary
FROM (
  SELECT
    origin,
    dest,
    uniquecarrier,
    flightnum,
    DepTime,
    ArrDelay,
    FlightDate,
    rank() over (PARTITION BY origin, dest, yeard, monthd, dayofmonth ORDER BY ArrDelay) rnk
  FROM aviation
  WHERE DepTime < '1200' AND Yeard = 2008 AND Cancelled = 0
) leg1
JOIN (
  SELECT
    origin,
    dest,
    uniquecarrier,
    flightnum,
    DepTime,
    ArrDelay,
    FlightDate,
    rank() over (PARTITION BY origin, dest, yeard, monthd, dayofmonth ORDER BY ArrDelay) rnk
  FROM aviation
  WHERE DepTime > '1200' AND Yeard = 2008 AND Cancelled = 0
) leg2
ON leg1.rnk = 1 AND leg2.rnk = 1 AND leg1.dest = leg2.origin AND date_add(leg1.FlightDate, 2) = leg2.FlightDate;


SELECT *
FROM capstone_3_2
WHERE (itinerary = "CMI-ORD-LAX" AND flightdate1 = "2008-03-04")
  OR (itinerary = "JAX-DFW-CRP" AND flightdate1 = "2008-09-09")
  OR (itinerary = "SLC-BFL-LAX" AND flightdate1 = "2008-04-01")
  OR (itinerary = "LAX-SFO-PHX" AND flightdate1 = "2008-07-12")
  OR (itinerary = "DFW-ORD-DFW" AND flightdate1 = "2008-06-10")
  OR (itinerary = "LAX-ORD-JFK" AND flightdate1 = "2008-01-01")
ORDER BY itinerary;

-- Query ID = hadoop_20170808210030_9e12680c-91e4-49c4-aaf4-8fbdf1b5ab68
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1502175039084_0023)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     40         40        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 14.32 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x  capstone_t1_g3_q2.y capstone_t1_g3_q2.airline1  capstone_t1_g3_q2.flightnum1  capstone_t1_g3_q2.flightdate1 capstone_t1_g3_q2.departure1  capstone_t1_g3_q2.delay1  capstone_t1_g3_q2.z capstone_t1_g3_q2.airline2  capstone_t1_g3_q2.flightnum2  capstone_t1_g3_q2.flightdate2 capstone_t1_g3_q2.departure2  capstone_t1_g3_q2.delay2  capstone_t1_g3_q2.total_delay capstone_t1_g3_q2.itinerary
-- CMI  ORD MQ  4278  2008-03-04  0710  -14.0 LAX AA  607 2008-03-06  1952  -24.0 -38.0 CMI-ORD-LAX
-- JAX  DFW AA  845 2008-09-09  0722  1.0 CRP MQ  3627  2008-09-11  1648  -7.0  -6.0  JAX-DFW-CRP
-- SLC  BFL OO  3755  2008-04-01  1101  12.0  LAX OO  5429  2008-04-03  1509  6.0 18.0  SLC-BFL-LAX
-- LAX  SFO WN  3534  2008-07-12  0650  -13.0 PHX US  412 2008-07-14  1916  -19.0 -32.0 LAX-SFO-PHX
-- DFW  ORD UA  1104  2008-06-10  0658  -21.0 DFW AA  2341  2008-06-12  1650  -10.0 -31.0 DFW-ORD-DFW
-- LAX  ORD UA  944 2008-01-01  0700  1.0 JFK B6  918 2008-01-03  1853  -7.0  -6.0  LAX-ORD-JFK
-- Time taken: 14.887 seconds, Fetched: 1 row(s)