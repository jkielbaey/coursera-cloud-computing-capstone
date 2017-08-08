CREATE TABLE capstone_t1_g2_q3
    (flight       STRING,
    airline       STRING,
    avg_arr_delay DOUBLE,
    rank          BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "capstone_t1_g2_q3",
    "dynamodb.column.mapping"="flight:flight,airline:airline,ontime_arrival:ontime_arrival,rank:rank"
);

SET mapred.reduce.tasks=40;
SET hive.exec.reducers.max=40;
SET dynamodb.throughput.write.percent=1.0;
set hive.cli.print.header=true;

INSERT OVERWRITE TABLE capstone_t1_g2_q3
SELECT
  concat(ranked.x, "-", ranked.y) AS flight,
  ranked.airline,
  ranked.avg_arr_delay,
  ranked.rnk
FROM (
  SELECT
    *,
    rank() over (PARTITION BY x, y ORDER BY avg_arr_delay, airline) rnk
  FROM (
    SELECT
      origin AS x,
      dest AS y,
      uniquecarrier AS airline,
      round(avg(ArrDelay), 2) AS avg_arr_delay
    FROM aviation
    WHERE cancelled = 0 AND ArrDelay IS NOT NULL
    GROUP BY origin, dest, uniquecarrier
  ) unranked
) ranked
WHERE ranked.rnk <= 10

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
-- _col0	_col1	_col2	_col3
-- Time taken: 37.309 seconds

SELECT * FROM capstone_t1_g2_q3 WHERE flight IN
  ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX')
ORDER BY flight, rank;

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
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.avg_arr_delay	capstone_t1_g2_q3.rank
-- ATL-PHX	FL	4.55	1
-- ATL-PHX	US	6.29	2
-- ATL-PHX	HP	8.48	3
-- ATL-PHX	EA	8.95	4
-- ATL-PHX	DL	9.81	5
-- CMI-ORD	MQ	10.14	1
-- DFW-IAH	PA (1)	-1.6	1
-- DFW-IAH	EV	5.09	2
-- DFW-IAH	UA	5.41	3
-- DFW-IAH	CO	6.49	4
-- DFW-IAH	OO	7.56	5
-- DFW-IAH	XE	8.09	6
-- DFW-IAH	AA	8.38	7
-- DFW-IAH	DL	8.6	8
-- DFW-IAH	MQ	9.1	9
-- IND-CMH	CO	-2.55	1
-- IND-CMH	AA	5.5	2
-- IND-CMH	HP	5.7	3
-- IND-CMH	NW	5.76	4
-- IND-CMH	US	6.88	5
-- IND-CMH	DL	10.69	6
-- IND-CMH	EA	10.81	7
-- JFK-LAX	UA	3.31	1
-- JFK-LAX	HP	6.68	2
-- JFK-LAX	AA	6.9	3
-- JFK-LAX	DL	7.93	4
-- JFK-LAX	PA (1)	11.02	5
-- JFK-LAX	TW	11.7	6
-- LAX-SFO	TZ	-7.62	1
-- LAX-SFO	PS	-2.15	2
-- LAX-SFO	F9	-2.03	3
-- LAX-SFO	EV	6.96	4
-- LAX-SFO	AA	7.39	5
-- LAX-SFO	MQ	7.81	6
-- LAX-SFO	US	7.96	7
-- LAX-SFO	WN	8.79	8
-- LAX-SFO	CO	9.35	9
-- LAX-SFO	NW	9.85	10
-- Time taken: 4.846 seconds, Fetched: 38 row(s)
