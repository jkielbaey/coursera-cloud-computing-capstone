DROP TABLE capstone_t1_g3_q2;
CREATE TABLE capstone_t1_g3_q2 (
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

INSERT OVERWRITE TABLE capstone_t1_g3_q2
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
ON leg1.rnk = leg2.rnk AND leg1.dest = leg2.origin AND date_add(leg1.FlightDate, 2) = leg2.FlightDate
WHERE leg1.rnk = 1;



SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "CMI-ORD-LAX" AND flightdate1 = "2008-03-04";

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
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- CMI	ORD	MQ	4278	2008-03-04	0710	-14.0	LAX	AA	607	2008-03-06	1952	-24.0	-38.0	CMI-ORD-LAX
-- Time taken: 14.887 seconds, Fetched: 1 row(s)

SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "JAX-DFW-CRP" AND flightdate1 = "2008-09-09";

-- Query ID = hadoop_20170808210045_b3538fdd-601c-4aab-8eed-abc8f0964842
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
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 4.11 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- JAX	DFW	AA	845	2008-09-09	0722	1.0	CRP	MQ	3627	2008-09-11	1648	-7.0	-6.0	JAX-DFW-CRP
-- Time taken: 4.67 seconds, Fetched: 1 row(s)

SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "SLC-BFL-LAX" AND flightdate1 = "2008-04-01";

-- Query ID = hadoop_20170808210050_476db1ed-3736-49d3-a65d-e6da41293069
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
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 3.14 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- SLC	BFL	OO	3755	2008-04-01	1101	12.0	LAX	OO	5429	2008-04-03	1509	6.0	18.0	SLC-BFL-LAX
-- Time taken: 3.729 seconds, Fetched: 1 row(s)

SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "LAX-SFO-PHX" AND flightdate1 = "2008-07-12";

-- Query ID = hadoop_20170808210206_efb40cc6-79a7-40bb-a520-81b34aff39bc
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
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 12.87 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- LAX	SFO	WN	3534	2008-07-12	0650	-13.0	PHX	US	412	2008-07-14	1916	-19.0	-32.0	LAX-SFO-PHX
-- Time taken: 13.479 seconds, Fetched: 1 row(s)

SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "DFW-ORD-DFW" AND flightdate1 = "2008-06-10";

-- Query ID = hadoop_20170808210219_a4af4ebf-5466-41c3-b154-94a6d212cc3a
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
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 3.59 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- DFW	ORD	UA	1104	2008-06-10	0658	-21.0	DFW	AA	2341	2008-06-12	1650	-10.0	-31.0	DFW-ORD-DFW
-- Time taken: 4.172 seconds, Fetched: 1 row(s)

SELECT * FROM capstone_t1_g3_q2 WHERE itinerary = "LAX-ORD-JFK" AND flightdate1 = "2008-01-01";

-- Query ID = hadoop_20170808210223_5c11c527-06bf-4a05-87d9-b02623e479d6
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
-- VERTICES: 01/01  [==========================>>] 100%  ELAPSED TIME: 3.16 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g3_q2.x	capstone_t1_g3_q2.y	capstone_t1_g3_q2.airline1	capstone_t1_g3_q2.flightnum1	capstone_t1_g3_q2.flightdate1	capstone_t1_g3_q2.departure1	capstone_t1_g3_q2.delay1	capstone_t1_g3_q2.z	capstone_t1_g3_q2.airline2	capstone_t1_g3_q2.flightnum2	capstone_t1_g3_q2.flightdate2	capstone_t1_g3_q2.departure2	capstone_t1_g3_q2.delay2	capstone_t1_g3_q2.total_delay	capstone_t1_g3_q2.itinerary
-- LAX	ORD	UA	944	2008-01-01	0700	1.0	JFK	B6	918	2008-01-03	1853	-7.0	-6.0	LAX-ORD-JFK
-- Time taken: 3.724 seconds, Fetched: 1 row(s)
