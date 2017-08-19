from __future__ import print_function

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == '__main__':
    LINE_FIELDS = ("DayOfWeek,Origin,UniqueCarried,FlightNum,Dest," +
                   "ArrDelayMinutes,FlightDate,ArrDelay,Month," +
                   "DepDelayMinutes,DepDelay,CRSArrTime,Year,Cancelled," +
                   "ArrTime,DepTime,CRSDepTime,DayofMonth").split(",")

    sc = SparkContext(appName="Solution1_2")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("/tmp/streaming")

    def convert_lines_to_dict(line):
        items = line.split(",")
        return {LINE_FIELDS[i]: items[i] for i in range(0, len(LINE_FIELDS))}

    def do_map(event):
        # print("MAP %s" % str(event))
        try:
            arrdelay = float(event['ArrDelay'])
        except ValueError:
            arrdelay = 0

        data = {
            'ArrDelay': arrdelay,
            'NrFlights': 1,
            'AvgDelay': arrdelay
        }
        return (event['UniqueCarried'], data)

    def do_reduce(a, b):
        # print("REDUCE\n  A: %s\n  B: %s" % (str(a), str(b)))
        output = {}
        output['ArrDelay'] = a['ArrDelay'] + b['ArrDelay']
        output['NrFlights'] = a['NrFlights'] + b['NrFlights']
        output['AvgDelay'] = output['ArrDelay'] / output['NrFlights']
        return output

    def do_update(new_values, rolling_values):
        output = None
        if rolling_values is None:
            output = new_values
        elif len(new_values) == 0:
            output = rolling_values
        else:
            output = [{}]
            output[0]['ArrDelay'] = new_values[0]['ArrDelay'] + rolling_values[0]['ArrDelay']
            output[0]['NrFlights'] = new_values[0]['NrFlights'] + rolling_values[0]['NrFlights']
            output[0]['AvgDelay'] = output[0]['ArrDelay'] / output[0]['NrFlights']
        # print("UPDATE: %s" % str(output))
        return output

    def get_key(item):
        return item[1][0]['AvgDelay']

    def print_results(time, rdd):
        print("--- %s ---" % str(time))
        # ordered_rdd = rdd.sortBy(keyfunc=get_key, ascending=True).take(10)
        for record in rdd.take(10):
            print(','.join([record[0], str(record[1][0]['AvgDelay'])]))

    lines = ssc.textFileStream(sys.argv[1])
    processed = lines.map(convert_lines_to_dict)\
                     .map(do_map)\
                     .reduceByKey(do_reduce)\
                     .updateStateByKey(do_update)
    ordered = processed.transform(lambda rdd: rdd.sortBy(lambda x: x[1][0]['AvgDelay'], ascending=True))
    ordered.foreachRDD(print_results)

    ssc.start()
    ssc.awaitTermination()
