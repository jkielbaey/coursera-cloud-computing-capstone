CREATE EXTERNAL TABLE capstone_t1_g2_q1
    (airport      STRING,
    airline       STRING,
    avg_dep_delay DOUBLE,
    rank          BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "capstone_t1_g2_q1",
    "dynamodb.column.mapping"="airport:airport,airline:airline,ontime_departure:ontime_departure,rank:rank"
);

SET mapred.reduce.tasks=40;
SET hive.exec.reducers.max=40;
SET dynamodb.throughput.write.percent=1.0;
set hive.cli.print.header=true;

INSERT OVERWRITE TABLE capstone_t1_g2_q1
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
-- _col0	_col1	_col2	_col3
-- Time taken: 38.618 seconds

SELECT * FROM capstone_t1_g2_q1 WHERE airport IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO') ORDER BY airport, rank;

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
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.avg_dep_delay	capstone_t1_g2_q1.rank
-- BWI	F9	0.76	1
-- BWI	PA (1)	4.76	2
-- BWI	CO	5.18	3
-- BWI	YV	5.5	4
-- BWI	NW	5.71	5
-- BWI	AA	6.0	6
-- BWI	9E	7.24	7
-- BWI	US	7.49	8
-- BWI	DL	7.68	9
-- BWI	UA	7.74	10
-- CMI	OH	0.61	1
-- CMI	US	2.03	2
-- CMI	TW	4.12	3
-- CMI	PI	4.46	4
-- CMI	DH	6.03	5
-- CMI	EV	6.67	6
-- CMI	MQ	8.02	7
-- IAH	NW	3.56	1
-- IAH	PA (1)	3.98	2
-- IAH	PI	3.99	3
-- IAH	US	5.06	4
-- IAH	F9	5.55	5
-- IAH	AA	5.7	6
-- IAH	TW	6.05	7
-- IAH	WN	6.23	8
-- IAH	OO	6.59	9
-- IAH	MQ	6.71	10
-- LAX	MQ	2.41	1
-- LAX	OO	4.22	2
-- LAX	FL	4.73	3
-- LAX	TZ	4.76	4
-- LAX	PS	4.86	5
-- LAX	NW	5.12	6
-- LAX	F9	5.73	7
-- LAX	HA	5.81	8
-- LAX	YV	6.02	9
-- LAX	US	6.75	10
-- MIA	9E	-3.0	1
-- MIA	EV	1.2	2
-- MIA	TZ	1.78	3
-- MIA	XE	1.87	4
-- MIA	PA (1)	4.2	5
-- MIA	NW	4.5	6
-- MIA	US	6.09	7
-- MIA	UA	6.87	8
-- MIA	ML (1)	7.5	9
-- MIA	FL	8.57	10
-- SFO	TZ	3.95	1
-- SFO	MQ	4.85	2
-- SFO	F9	5.16	3
-- SFO	PA (1)	5.29	4
-- SFO	NW	5.76	5
-- SFO	PS	6.3	6
-- SFO	DL	6.56	7
-- SFO	CO	7.08	8
-- SFO	US	7.53	9
-- SFO	TW	7.79	10
-- Time taken: 4.579 seconds, Fetched: 57 row(s)
