SELECT *
FROM (
  SELECT
    total.origin AS airport,
    total.uniquecarrier AS airline,
    100.0 * ontime.nr_flights / total.nr_flights AS ontime_departure,
    rank() over (PARTITION BY total.origin ORDER BY ontime.nr_flights / total.nr_flights DESC) AS rank
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
) t
WHERE rank <= 10
DISTRIBUTE BY airport SORT BY airport, rank;
