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
-- Time taken: 62.204 seconds, Fetched: 10 row(s)
