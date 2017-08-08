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

-- /////////

SELECT
  total.uniquecarrier AS airline,
  100.0 * ontime.nr_flights / total.nr_flights AS ontime_arrival
FROM
  (SELECT uniquecarrier, count(flightnum) AS nr_flights FROM aviation WHERE cancelled = 0 GROUP BY uniquecarrier) total
  JOIN
  (SELECT uniquecarrier, count(flightnum) AS nr_flights FROM aviation WHERE cancelled = 0 AND arrdelayminutes = 0 GROUP BY uniquecarrier) ontime
  ON total.uniquecarrier = ontime.uniquecarrier
ORDER BY ontime_arrival DESC
LIMIT 10;

-- hive> SELECT
--     >   total.uniquecarrier AS airline,
--     >   100.0 * ontime.nr_flights / total.nr_flights AS ontime_arrival
--     > FROM
--     >   (SELECT uniquecarrier, count(flightnum) AS nr_flights FROM aviation WHERE cancelled = 0 GROUP BY uniquecarrier) total
--     >   JOIN
--     >   (SELECT uniquecarrier, count(flightnum) AS nr_flights FROM aviation WHERE cancelled = 0 AND arrdelayminutes = 0 GROUP BY uniquecarrier) ontime
--     >   ON total.uniquecarrier = ontime.uniquecarrier
--     > ORDER BY ontime_arrival DESC
--     > LIMIT 10;
-- Query ID = hadoop_20170728200105_35f5ccda-22d7-4a25-9bef-ba3270369554
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501268061716_0007)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Map 5 .......... container     SUCCEEDED     67         67        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     16         16        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED     12         12        0        0       0       0
-- Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0
-- Reducer 6 ...... container     SUCCEEDED      8          8        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 06/06  [==========================>>] 100%  ELAPSED TIME: 29.50 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- airline	ontime_arrival
-- HA	75.44692416644337
-- AQ	62.09087789684844
-- 9E	61.17799009429182
-- DH	60.80051157376008
-- PS	59.5787602968371
-- OO	58.82374145368539
-- TZ	58.64986682062752
-- WN	57.1566299766753
-- ML (1)	57.14203233256351
-- YV	55.97692555574944
-- Time taken: 31.461 seconds, Fetched: 10 row(s)
