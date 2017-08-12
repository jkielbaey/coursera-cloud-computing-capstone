SELECT
  DayOfWeek AS Weekday,
  round(avg(ArrDelay),2) as avg_delay
FROM aviation
WHERE cancelled = 0
GROUP BY DayOfWeek
ORDER BY avg_delay;

-- Query ID = hadoop_20170808185910_58c60324-1b01-4def-a899-b5d91c639bf9
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
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 29.01 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- 6	4.3
-- 2	5.99
-- 7	6.61
-- 1	6.72
-- 3	7.2
-- 4	9.09
-- 5	9.72
-- Time taken: 29.936 seconds, Fetched: 7 row(s)
