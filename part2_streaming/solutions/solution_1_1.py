from __future__ import print_function

import sys
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

    def do_update(new_values, rolling_sum):
        return sum(new_values) + (rolling_sum or 0)

    def print_results(time, rdd):
        print("--- %s ---" % str(time))
        for record in rdd.take(10):
            print(','.join([record[0], str(record[1])]))

    lines = ssc.textFileStream(sys.argv[1])
    parsed = lines.map(convert_lines_to_dict)
    processed = parsed.flatMap(lambda r: ((r['Origin'], 1), (r['Dest'], 1))) \
                      .reduceByKey(lambda a, b: a + b) \
                      .updateStateByKey(do_update)
    ordered = processed.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    ordered.foreachRDD(print_results)

    ssc.start()
    ssc.awaitTermination()
