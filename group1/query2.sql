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
-- HA	-1.01
-- AQ	1.16
-- PS	1.45
-- ML (1)	4.75
-- PA (1)	5.32
-- F9	5.47
-- WN	5.56
-- NW	5.56
-- OO	5.74
-- 9E	5.87
-- Time taken: 30.122 seconds, Fetched: 10 row(s)
