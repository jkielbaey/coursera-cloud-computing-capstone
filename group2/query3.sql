CREATE EXTERNAL TABLE capstone_t1_g2_q3
    (flight          STRING,
    airline          STRING,
    ontime_arrival   DOUBLE,
    rank             BIGINT)
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
  concat(ranked.origin, "-", ranked.destination) AS flight,
  ranked.airline,
  ranked.ontime_arrival,
  ranked.rnk
FROM (
  SELECT
    unranked.origin,
    unranked.destination,
    unranked.airline,
    unranked.ontime_arrival,
    rank() over (PARTITION BY unranked.origin, unranked.destination ORDER BY unranked.ontime_arrival DESC, unranked.airline) rnk
  FROM (
    SELECT
      total.origin AS origin,
      total.dest AS destination,
      total.uniquecarrier AS airline,
      100.0 * ontime.nr_flights / total.nr_flights AS ontime_arrival
    FROM
      (
        SELECT origin, dest, uniquecarrier, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0
        GROUP BY origin, dest, uniquecarrier
      ) total
      JOIN
      (
        SELECT origin, dest, uniquecarrier, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0 AND arrdelayminutes = 0
        GROUP BY origin, dest, uniquecarrier
      ) ontime
      ON total.dest = ontime.dest AND total.origin = ontime.origin AND total.uniquecarrier = ontime.uniquecarrier
    ) unranked
  ) ranked
WHERE ranked.rnk <= 10
DISTRIBUTE BY flight SORT BY flight, ranked.rnk;

-- hive> INSERT OVERWRITE TABLE capstone_t1_g2_q3
--     > SELECT
--     >   concat(ranked.origin, "-", ranked.destination) AS flight,
--     >   ranked.airline,
--     >   ranked.ontime_arrival,
--     >   ranked.rnk
--     > FROM (
--     >   SELECT
--     >     unranked.origin,
--     >     unranked.destination,
--     >     unranked.airline,
--     >     unranked.ontime_arrival,
--     >     rank() over (PARTITION BY unranked.origin, unranked.destination ORDER BY unranked.ontime_arrival DESC, unranked.airline) rnk
--     >   FROM (
--     >     SELECT
--     >       total.origin AS origin,
--     >       total.dest AS destination,
--     >       total.uniquecarrier AS airline,
--     >       100.0 * ontime.nr_flights / total.nr_flights AS ontime_arrival
--     >     FROM
--     >       (
--     >         SELECT origin, dest, uniquecarrier, count(flightnum) AS nr_flights
--     >         FROM aviation
--     >         WHERE cancelled = 0
--     >         GROUP BY origin, dest, uniquecarrier
--     >       ) total
--     >       JOIN
--     >       (
--     >         SELECT origin, dest, uniquecarrier, count(flightnum) AS nr_flights
--     >         FROM aviation
--     >         WHERE cancelled = 0 AND arrdelayminutes = 0
--     >         GROUP BY origin, dest, uniquecarrier
--     >       ) ontime
--     >       ON total.dest = ontime.dest AND total.origin = ontime.origin AND total.uniquecarrier = ontime.uniquecarrier
--     >     ) unranked
--     >   ) ranked
--     > WHERE ranked.rnk <= 10
--     > DISTRIBUTE BY flight SORT BY flight, ranked.rnk;
-- Query ID = hadoop_20170802193824_434eaa6f-cffa-4679-8aa2-28c1361876b3
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501696989350_0008)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Map 6 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 4 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 5 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 7 ...... container     SUCCEEDED     40         40        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 07/07  [==========================>>] 100%  ELAPSED TIME: 169.52 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- _col0	_col1	_col2	_col3
-- Time taken: 170.578 seconds
--
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'CMI-ORD';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- CMI-ORD	MQ	58.91246684350133	1
-- Time taken: 4.53 seconds, Fetched: 1 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'IND-CMH';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- IND-CMH	CO	72.86606523247745	1
-- IND-CMH	HP	55.01567398119122	2
-- IND-CMH	EA	46.26168224299065	3
-- IND-CMH	NW	40.92307692307692	4
-- IND-CMH	DL	37.98076923076923	5
-- IND-CMH	US	32.67060720042988	6
-- IND-CMH	AA	25.0	7
-- Time taken: 0.915 seconds, Fetched: 7 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'DFW-IAH';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- DFW-IAH	PA (1)	75.43859649122807	1
-- DFW-IAH	EV	60.14957264957265	2
-- DFW-IAH	OO	57.592592592592595	3
-- DFW-IAH	UA	57.396449704142015	4
-- DFW-IAH	XE	55.21051895369982	5
-- DFW-IAH	CO	54.6494022647699	6
-- DFW-IAH	AA	52.22136188987421	7
-- DFW-IAH	MQ	49.08466819221968	8
-- DFW-IAH	DL	38.63098991172762	9
-- Time taken: 0.978 seconds, Fetched: 9 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'LAX-SFO';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- LAX-SFO	F9	76.6	1
-- LAX-SFO	PS	70.04530011325028	2
-- LAX-SFO	WN	62.29370629370629	3
-- LAX-SFO	TZ	61.904761904761905	4
-- LAX-SFO	EV	58.52090032154341	5
-- LAX-SFO	AA	56.13664425847167	6
-- LAX-SFO	UA	53.99041672716713	7
-- LAX-SFO	XE	53.234042553191486	8
-- LAX-SFO	CO	51.98191933240612	9
-- LAX-SFO	MQ	49.44649446494465	10
-- Time taken: 0.758 seconds, Fetched: 10 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'JFK-LAX';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- JFK-LAX	UA	57.166435439889014	1
-- JFK-LAX	AA	52.68340958499627	2
-- JFK-LAX	HP	51.996867658574786	3
-- JFK-LAX	DL	51.048525985209196	4
-- JFK-LAX	TW	44.45633022904606	5
-- JFK-LAX	PA (1)	42.93419633225459	6
-- Time taken: 0.806 seconds, Fetched: 6 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q3 WHERE flight = 'ATL-PHX';
-- OK
-- capstone_t1_g2_q3.flight	capstone_t1_g2_q3.airline	capstone_t1_g2_q3.ontime_arrival	capstone_t1_g2_q3.rank
-- ATL-PHX	FL	55.04201680672269	1
-- ATL-PHX	EA	48.2566953006569	2
-- ATL-PHX	US	46.56451999042375	3
-- ATL-PHX	HP	44.405334055371284	4
-- ATL-PHX	DL	40.49737558593202	5
-- Time taken: 0.669 seconds, Fetched: 5 row(s)
