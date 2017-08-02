CREATE EXTERNAL TABLE capstone_t1_g2_q2
    (airport         STRING,
    destination      STRING,
    ontime_departure DOUBLE,
    rank             BIGINT)
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
    unranked.origin,
    unranked.destination,
    unranked.ontime_departure,
    rank() over (PARTITION BY unranked.origin ORDER BY unranked.ontime_departure DESC, unranked.destination) rnk
  FROM (
    SELECT
      total.origin AS origin,
      total.dest AS destination,
      100.0 * ontime.nr_flights / total.nr_flights AS ontime_departure
    FROM
      (
        SELECT origin, dest, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0
        GROUP BY origin, dest
      ) total
      JOIN
      (
        SELECT origin, dest, count(flightnum) AS nr_flights
        FROM aviation
        WHERE cancelled = 0 AND depdelayminutes = 0
        GROUP BY origin, dest
      ) ontime
      ON total.dest = ontime.dest AND total.origin = ontime.origin
    ) unranked
  ) ranked
WHERE ranked.rnk <= 10
DISTRIBUTE BY ranked.origin SORT BY ranked.origin, ranked.rnk;

-- hive> INSERT OVERWRITE TABLE capstone_t1_g2_q2
--     > SELECT *
--     > FROM (
--     >   SELECT
--     >     unranked.origin,
--     >     unranked.destination,
--     >     unranked.ontime_departure,
--     >     rank() over (PARTITION BY unranked.origin ORDER BY unranked.ontime_departure DESC, unranked.destination) rnk
--     >   FROM (
--     >     SELECT
--     >       total.origin AS origin,
--     >       total.dest AS destination,
--     >       100.0 * ontime.nr_flights / total.nr_flights AS ontime_departure
--     >     FROM
--     >       (
--     >         SELECT origin, dest, count(flightnum) AS nr_flights
--     >         FROM aviation
--     >         WHERE cancelled = 0
--     >         GROUP BY origin, dest
--     >       ) total
--     >       JOIN
--     >       (
--     >         SELECT origin, dest, count(flightnum) AS nr_flights
--     >         FROM aviation
--     >         WHERE cancelled = 0 AND depdelayminutes = 0
--     >         GROUP BY origin, dest
--     >       ) ontime
--     >       ON total.dest = ontime.dest AND total.origin = ontime.origin
--     >     ) unranked
--     >   ) ranked
--     > WHERE ranked.rnk <= 10
--     > DISTRIBUTE BY ranked.origin SORT BY ranked.origin, ranked.rnk;
-- Query ID = hadoop_20170802185302_73cc3505-e0de-40e1-89d2-1ec9a4f34369
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501696989350_0004)
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
-- VERTICES: 07/07  [==========================>>] 100%  ELAPSED TIME: 43.83 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- _col0	_col1	_col2	_col3
-- Time taken: 44.981 seconds
--
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'CMI';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- CMI	ABI	100.0	1
-- CMI	STL	86.53386454183267	2
-- CMI	PIA	83.24324324324324	3
-- CMI	DFW	82.38944918541505	4
-- CMI	CVG	81.21755545068429	5
-- CMI	ATL	78.44036697247707	6
-- CMI	ORD	73.99867374005305	7
-- CMI	DAY	65.92941176470588	8
-- CMI	PIT	64.93055555555556	9
-- Time taken: 0.143 seconds, Fetched: 9 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'BWI';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- BWI	SAV	100.0	1
-- BWI	IAD	82.22996515679442	2
-- BWI	DAB	76.35135135135135	3
-- BWI	MLB	71.75141242937853	4
-- BWI	SRQ	70.83993660855785	5
-- BWI	MSP	70.08940426661945	6
-- BWI	MEM	69.68099129976272	7
-- BWI	MDT	69.47535771065183	8
-- BWI	DTW	67.91667520964468	9
-- BWI	DCA	66.11872146118722	10
-- Time taken: 0.119 seconds, Fetched: 10 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'MIA';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- MIA	SHV	100.0	1
-- MIA	HOU	77.05142231947484	2
-- MIA	SAN	77.04918032786885	3
-- MIA	MEM	76.75567506722531	4
-- MIA	MCI	74.14965986394557	5
-- MIA	AUS	73.30246913580247	6
-- MIA	DTW	73.02616864236188	7
-- MIA	MDW	71.13715277777777	8
-- MIA	SAV	71.0594315245478	9
-- MIA	MSY	70.69541822925159	10
-- Time taken: 0.115 seconds, Fetched: 10 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'LAX';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- LAX	DRO	100.0	1
-- LAX	IDA	100.0	2
-- LAX	LAX	100.0	3
-- LAX	MAF	100.0	4
-- LAX	PIH	100.0	5
-- LAX	RSW	100.0	6
-- LAX	SDF	100.0	7
-- LAX	VIS	84.41558441558442	8
-- LAX	PIE	79.94923857868021	9
-- LAX	RDM	79.71014492753623	10
-- Time taken: 0.127 seconds, Fetched: 10 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'IAH';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- IAH	MLI	100.0	1
-- IAH	MSN	100.0	2
-- IAH	AGS	86.7170626349892	3
-- IAH	HOU	80.06444382179882	4
-- IAH	JAC	80.0	5
-- IAH	EFD	77.11085582998277	6
-- IAH	BPT	76.47058823529412	7
-- IAH	OGG	75.26315789473684	8
-- IAH	VCT	74.83108108108108	9
-- IAH	LRD	74.52702702702703	10
-- Time taken: 0.11 seconds, Fetched: 10 row(s)
-- hive> SELECT * FROM capstone_t1_g2_q2 WHERE airport = 'SFO';
-- OK
-- capstone_t1_g2_q2.airport	capstone_t1_g2_q2.destination	capstone_t1_g2_q2.ontime_departure	capstone_t1_g2_q2.rank
-- SFO	FAR	100.0	1
-- SFO	PIH	100.0	2
-- SFO	SDF	100.0	3
-- SFO	PIE	87.86127167630057	4
-- SFO	OAK	85.80323785803238	5
-- SFO	LGA	81.81818181818181	6
-- SFO	MSO	75.0	7
-- SFO	MEM	74.65665785208564	8
-- SFO	MKE	72.5625	9
-- SFO	SJC	70.17368069472278	10
-- Time taken: 0.115 seconds, Fetched: 10 row(s)
