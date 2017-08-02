CREATE EXTERNAL TABLE capstone_t1_g2_q4
    (flight          STRING,
    avg_arrival_delay   DOUBLE)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "capstone_t1_g2_q4",
    "dynamodb.column.mapping"="flight:flight,avg_arrival_delay:avg_arrival_delay"
);


INSERT OVERWRITE TABLE capstone_t1_g2_q4
SELECT
  concat(origin, "-", dest) AS flight,
  avg(arrdelayminutes) AS avg_arrival_delay
FROM aviation
WHERE cancelled = 0 AND arrdelayminutes IS NOT NULL
GROUP BY origin, dest
ORDER BY avg_arrival_delay;

-- hive> INSERT OVERWRITE TABLE capstone_t1_g2_q4
--     > SELECT
--     >   concat(origin, "-", dest) AS flight,
--     >   avg(arrdelayminutes) AS avg_arrival_delay
--     > FROM aviation
--     > WHERE cancelled = 0 AND arrdelayminutes IS NOT NULL
--     > GROUP BY origin, dest
--     > ORDER BY avg_arrival_delay;
-- Query ID = hadoop_20170802200120_58c1b633-12cd-4e75-82c1-a6d49763a9fb
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501696989350_0009)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED     65         65        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED     40         40        0        0       0       0
-- Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 181.73 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- flight	avg_arrival_delay
-- Time taken: 183.558 seconds
--
-- hive> SELECT * FROM capstone_t1_g2_q4 WHERE flight IN ('CMI-ORD', 'IND-CMH', 'DFW-IAH', 'LAX-SFO', 'JFK-LAX', 'ATL-PHX') ORDER BY flight;
-- Query ID = hadoop_20170802200933_f1496561-6693-4515-9742-9b815b684373
-- Total jobs = 1
-- Launching Job 1 out of 1
--
--
-- Status: Running (Executing on YARN cluster with App id application_1501696989350_0010)
--
-- ----------------------------------------------------------------------------------------------
--         VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
-- ----------------------------------------------------------------------------------------------
-- Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
-- Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0
-- ----------------------------------------------------------------------------------------------
-- VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 6.53 s
-- ----------------------------------------------------------------------------------------------
-- OK
-- capstone_t1_g2_q4.flight	capstone_t1_g2_q4.avg_arrival_delay
-- ATL-PHX	13.620285929786068
-- CMI-ORD	15.739150630391507
-- DFW-IAH	11.060462025053555
-- IND-CMH	6.613487475915222
-- JFK-LAX	14.632357646666017
-- LAX-SFO	13.695194295129438
-- Time taken: 8.486 seconds, Fetched: 6 row(s)
