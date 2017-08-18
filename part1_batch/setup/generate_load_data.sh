#!/bin/bash
for y in `seq 1988 2008`
do
  for m in 01 02 03 04 05 06 07 08 09 10 11 12
  do
    echo "ALTER TABLE aviation ADD PARTITION(yeard=$y, monthd=$m) LOCATION 'hdfs:/INPUT/${y}/${m}/';"
  done
done
