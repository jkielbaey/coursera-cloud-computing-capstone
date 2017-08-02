CREATE EXTERNAL TABLE capstone_t1_g2_q1
    (airport         STRING,
    airline          STRING,
    ontime_departure DOUBLE,
    rank             BIGINT)
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
    unranked.origin,
    unranked.airline,
    unranked.ontime_departure,
    rank() over (PARTITION BY unranked.origin ORDER BY unranked.ontime_departure DESC, unranked.airline) rnk
  FROM (
    SELECT
      total.origin AS origin,
      total.uniquecarrier AS airline,
      100.0 * ontime.nr_flights / total.nr_flights AS ontime_departure
    FROM
      (
        SELECT origin, uniquecarrier, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0
        GROUP BY origin, uniquecarrier
      ) total
      JOIN
      (
        SELECT origin, uniquecarrier, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0 AND depdelayminutes = 0
        GROUP BY origin, uniquecarrier
      ) ontime
      ON total.uniquecarrier = ontime.uniquecarrier AND total.origin = ontime.origin
    ) unranked
  ) ranked
WHERE ranked.rnk <= 10
DISTRIBUTE BY ranked.origin SORT BY ranked.origin, ranked.rnk;

-- hive>
--     > INSERT OVERWRITE TABLE capstone_t1_g2_q1
--     > SELECT *
--     > FROM (
--     >   SELECT
--     >     total.origin AS airport,
--     >     total.uniquecarrier AS airline,
--     >     100.0 * ontime.nr_flights / total.nr_flights AS ontime_departure,
--     >     rank() over (PARTITION BY total.origin ORDER BY ontime.nr_flights / total.nr_flights DESC) AS rank
--     >   FROM
--     >     (
--     >       SELECT origin, uniquecarrier, count(flightnum) AS nr_flights
--     >       FROM aviation
--     >       WHERE cancelled = 0
--     >       GROUP BY origin, uniquecarrier
--     >     ) total
--     >     JOIN
--     >     (
--     >       SELECT origin, uniquecarrier, count(flightnum) AS nr_flights
--     >       FROM aviation
--     >       WHERE cancelled = 0 AND depdelayminutes = 0
--     >       GROUP BY origin, uniquecarrier
--     >     ) ontime
--     >     ON total.uniquecarrier = ontime.uniquecarrier AND total.origin = ontime.origin
--     > ) t
--     > WHERE rank <= 10
--     > DISTRIBUTE BY airport SORT BY airport, rank;
-- Query ID = hadoop_20170731200347_24e87847-7222-44ff-8d54-68a628a83d73
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501525542611_0005)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Map 6 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 4 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 5 ...... container     SUCCEEDED     20         20        0        0       0       0
-- Reducer 7 ...... container     SUCCEEDED     20         20        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 07/07  [==========================>>] 100%  ELAPSED TIME: 43.52 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- Time taken: 44.37 seconds
--
-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'CMI';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- CMI	TW	85.78461538461538	1
-- CMI	OH	83.9208410636982	2
-- CMI	EV	78.44036697247707	3
-- CMI	MQ	74.6609651802077	4
-- CMI	DH	72.50996015936255	5
-- CMI	US	65.93581188433429	6
-- CMI	PI	65.57474687313878	7
-- CMI	DAY	65.92941176470588	8
-- CMI	PIT	64.93055555555556	9
-- Time taken: 0.104 seconds, Fetched: 9 row(s)

-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'BWI';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- BWI	F9	76.62337662337663	1
-- BWI	YV	76.3014763014763	2
-- BWI	EA	75.92833876221498	3
-- BWI	9E	72.9126213592233	4
-- BWI	NW	72.019030627416	5
-- BWI	FL	69.98039536023525	6
-- BWI	CO	68.89466080595088	7
-- BWI	OH	68.75224998200014	8
-- BWI	PA (1)	67.61904761904762	9
-- BWI	AA	66.0800778597619	10
-- Time taken: 0.125 seconds, Fetched: 10 row(s)

-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'MIA';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- MIA	EV	80.61674008810573	1
-- MIA	PA (1)	79.70422052906429	2
-- MIA	ML (1)	76.74418604651163	3
-- MIA	TZ	76.72465506898621	4
-- MIA	9E	75.0	5
-- MIA	NW	74.91917311648868	6
-- MIA	XE	73.1909028256375	7
-- MIA	FL	67.9519595448799	8
-- MIA	EA	66.26441342607721	9
-- MIA	TW	64.976848536923	10
-- Time taken: 0.103 seconds, Fetched: 10 row(s)

-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'LAX';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- LAX	YV	78.47630845462992	1
-- LAX	MQ	76.80148499270992	2
-- LAX	FL	71.82890855457227	3
-- LAX	NW	70.15062524408641	4
-- LAX	TZ	68.96724181045262	5
-- LAX	OO	68.9061610559969	6
-- LAX	ML (1)	68.86721680420105	7
-- LAX	EA	68.45464215164868	8
-- LAX	HA	67.45417515274949	9
-- LAX	PA (1)	63.31097490459119	10
-- Time taken: 0.1 seconds, Fetched: 10 row(s)

-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'IAH';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- IAH	PA (1)	82.23030303030303	1
-- IAH	NW	77.86821919320126	2
-- IAH	WN	76.5308891367926	3
-- IAH	TW	75.29619359717671	4
-- IAH	EA	74.24242424242425	5
-- IAH	YV	70.24094463060476	6
-- IAH	AA	70.173084769168	7
-- IAH	9E	70.04761904761905	8
-- IAH	MQ	69.40298507462687	9
-- IAH	OH	67.86407766990291	10
-- Time taken: 0.102 seconds, Fetched: 10 row(s)

-- hive> SELECT * FROM capstone_t1_g2_q1 WHERE airport = 'SFO';
-- OK
-- capstone_t1_g2_q1.airport	capstone_t1_g2_q1.airline	capstone_t1_g2_q1.ontime_departure	capstone_t1_g2_q1.rank
-- SFO	PA (1)	77.92665726375176	1
-- SFO	TZ	76.8268997329449	2
-- SFO	MQ	71.99960811207994	3
-- SFO	EA	71.89574625030735	4
-- SFO	YV	68.20175438596492	5
-- SFO	NW	67.9873493647418	6
-- SFO	HP	64.86019072248263	7
-- SFO	EV	64.69760900140648	8
-- SFO	OH	64.68085106382979	9
-- SFO	F9	64.46138711264142	10
-- Time taken: 0.123 seconds, Fetched: 10 row(s)
