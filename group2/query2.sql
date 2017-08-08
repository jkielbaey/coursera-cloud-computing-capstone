CREATE TABLE capstone_t1_g2_q2
    (origin       STRING,
    destination   STRING,
    avg_dep_delay DOUBLE,
    rank          BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "capstone_t1_g2_q2",
    "dynamodb.column.mapping"="airport:airport,destination:destination,ontime_departure:ontime_departure,rank:rank"
);

SET mapred.reduce.tasks=40;
SET hive.exec.reducers.max=40;
SET dynamodb.throughput.write.percent=1.0;
set hive.cli.print.header=true;

INSERT OVERWRITE TABLE capstone_t1_g2_q2
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
-- _col0	_col1	_col2	_col3
-- Time taken: 36.006 seconds

SELECT * FROM capstone_t1_g2_q2 WHERE origin IN ('CMI', 'BWI', 'MIA', 'LAX', 'IAH', 'SFO') ORDER BY origin, rank;

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
-- capstone_t1_g2_q2.origin	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.avg_dep_delay	capstone_t1_g2_q2.rank
-- BWI	SAV	-7.0	1
-- BWI	MLB	1.16	2
-- BWI	DAB	1.47	3
-- BWI	SRQ	1.59	4
-- BWI	IAD	1.79	5
-- BWI	UCA	3.65	6
-- BWI	CHO	3.74	7
-- BWI	GSP	4.2	8
-- BWI	SJU	4.44	9
-- BWI	OAJ	4.47	10
-- CMI	ABI	-7.0	1
-- CMI	PIT	1.1	2
-- CMI	CVG	1.89	3
-- CMI	DAY	3.12	4
-- CMI	STL	3.98	5
-- CMI	PIA	4.59	6
-- CMI	DFW	5.94	7
-- CMI	ATL	6.67	8
-- CMI	ORD	8.19	9
-- IAH	MSN	-2.0	1
-- IAH	AGS	-0.62	2
-- IAH	MLI	-0.5	3
-- IAH	EFD	1.89	4
-- IAH	HOU	2.17	5
-- IAH	JAC	2.57	6
-- IAH	MTJ	2.95	7
-- IAH	RNO	3.22	8
-- IAH	BPT	3.6	9
-- IAH	VCT	3.61	10
-- LAX	SDF	-16.0	1
-- LAX	IDA	-7.0	2
-- LAX	DRO	-6.0	3
-- LAX	RSW	-3.0	4
-- LAX	LAX	-2.0	5
-- LAX	BZN	-0.73	6
-- LAX	MAF	0.0	7
-- LAX	PIH	0.0	8
-- LAX	IYK	1.27	9
-- LAX	MFE	1.38	10
-- MIA	SHV	0.0	1
-- MIA	BUF	1.0	2
-- MIA	SAN	1.71	3
-- MIA	SLC	2.54	4
-- MIA	HOU	2.91	5
-- MIA	ISP	3.65	6
-- MIA	MEM	3.75	7
-- MIA	PSE	3.98	8
-- MIA	TLH	4.26	9
-- MIA	MCI	4.61	10
-- SFO	SDF	-10.0	1
-- SFO	MSO	-4.0	2
-- SFO	PIH	-3.0	3
-- SFO	LGA	-1.76	4
-- SFO	PIE	-1.34	5
-- SFO	OAK	-0.81	6
-- SFO	FAR	0.0	7
-- SFO	BNA	2.43	8
-- SFO	MEM	3.3	9
-- SFO	SCK	4.0	10
-- Time taken: 5.912 seconds, Fetched: 59 row(s)
