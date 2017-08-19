from __future__ import print_function

import sys
import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == '__main__':
    LINE_FIELDS = ("DayOfWeek,Origin,UniqueCarried,FlightNum,Dest," +
                   "ArrDelayMinutes,FlightDate,ArrDelay,Month," +
                   "DepDelayMinutes,DepDelay,CRSArrTime,Year,Cancelled," +
                   "ArrTime,DepTime,CRSDepTime,DayofMonth").split(",")

    sc = SparkContext(appName="Solution1_1")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("/tmp/streaming")

    def convert_lines_to_dict(line):
        items = line.split(",")
        return {LINE_FIELDS[i]: items[i] for i in range(0, len(LINE_FIELDS))}

    def do_map(event):
        return ((event['Origin'], 1), (event['Dest'], 1))

    def do_reduce(a, b):
        return a + b

    def do_update(new_values, rolling_sum):
        return sum(new_values) + (rolling_sum or 0)

    def print_results(rdd):
        print('--- %s - Results ---' % datetime.datetime.now())
        ordered_rdd = rdd.sortBy(lambda x: x[1], ascending=False).take(10)
        for record in ordered_rdd:
            print(','.join([record[0], str(record[1])]))

    lines = ssc.textFileStream(sys.argv[1])
    process = lines.map(convert_lines_to_dict) \
                   .flatMap(do_map) \
                   .reduceByKey(do_reduce) \
                   .updateStateByKey(do_update)
    process.foreachRDD(print_results)

    ssc.start()
    ssc.awaitTermination()
