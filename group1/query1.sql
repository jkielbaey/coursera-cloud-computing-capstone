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


-- hive> SELECT
--     >   o.origin AS airport,
--     >   o.total_departing_flights + d.total_arriving_flights AS total_flights
--     > FROM
--     >   ( SELECT origin, count(flightnum) AS total_departing_flights FROM aviation GROUP BY origin) o
--     >   JOIN ( SELECT dest, count(flightnum) AS total_arriving_flights FROM aviation GROUP BY dest) d
--     >   ON o.origin = d.dest
--     > ORDER BY total_flights DESC
--     > LIMIT 10;
-- Query ID = hadoop_20170728194903_9fa373b8-04b9-4225-98be-48553ad54e7d
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501268061716_0006)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Map 5 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     32         32        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     32         32        0        0       0       0
-- Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0
-- Reducer 6 ...... container     SUCCEEDED     32         32        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 06/06  [==========================>>] 100%  ELAPSED TIME: 29.74 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- ORD	12449354
-- ATL	11540422
-- DFW	10799303
-- LAX	7723596
-- PHX	6585534
-- DEN	6273787
-- DTW	5636622
-- IAH	5480734
-- MSP	5199213
-- SFO	5171023
-- Time taken: 30.985 seconds, Fetched: 10 row(s)
