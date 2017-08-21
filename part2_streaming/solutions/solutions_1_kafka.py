from __future__ import print_function

# cd /usr/lib/spark/
# bin/spark-submit --jars external/lib/spark-streaming-kafka-0-8-assembly.jar \
#   --executor-memory 20G --master yarn --num-executors 50 \
#   ~/sol_1_1_kafka.py \
#   ip-10-1-0-22:9092,ip-10-1-1-170:9092,ip-10-1-2-220:9092 \
#   test-day

import sys
import uuid
import json
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == '__main__':
    conf = SparkConf()
    conf.setAppName("Solution1_1_kafka")
    conf.set("spark.streaming.kafka.maxRatePerPartition", 50000)
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.python.worker.memory", "1g")

    def s11_print_results(time, rdd):
        print("--- %s :: SOLUTION 1.1 ---" % str(time))
        for record in rdd.take(10):
            print(','.join([record[0], str(record[1])]))

    def s12_do_map(event):
        try:
            arrdelay = float(event['ArrDelay'])
        except ValueError:
            arrdelay = 0

        data = {
            'ArrDelay': arrdelay,
            'NrFlights': 1,
            'AvgDelay': arrdelay
        }
        return (event['UniqueCarrier'], data)

    def s13_do_map(event):
        try:
            arrdelay = float(event['ArrDelay'])
        except ValueError:
            arrdelay = 0

        data = {
            'ArrDelay': arrdelay,
            'NrFlights': 1,
            'AvgDelay': arrdelay
        }
        return (event['DayOfWeek'], data)

    def s12_do_reduce(a, b):
        output = {}
        output['ArrDelay'] = a['ArrDelay'] + b['ArrDelay']
        output['NrFlights'] = a['NrFlights'] + b['NrFlights']
        output['AvgDelay'] = output['ArrDelay'] / output['NrFlights']
        return output

    def s12_do_update(new_values, rolling_values):
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
        return output

    def s12_print_results(time, rdd):
        print("--- %s :: SOLUTION 1.2 ---" % str(time))
        for record in rdd.take(10):
            print(','.join([record[0], str(record[1][0]['AvgDelay'])]))

    def s13_print_results(time, rdd):
        print("--- %s :: SOLUTION 1.3 ---" % str(time))
        for record in rdd.take(10):
            print(','.join([record[0], str(record[1][0]['AvgDelay'])]))

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("/tmp/streaming")

    brokers, topic = sys.argv[1:]
    kafka_consumer_group = str(uuid.uuid4())
    kafka_client_params = {
        "metadata.broker.list": brokers,
        "group.id": kafka_consumer_group,
        "auto.offset.reset": "smallest",
    }
    # kafkaStreams = [KafkaUtils.createDirectStream(ssc, [topic + str(_)], kafka_client_params) for _ in range(1, 7)]
    # events = ssc.union(*kafkaStreams)
    events = KafkaUtils.createDirectStream(ssc, [topic], kafka_client_params)
    parsed = events.map(lambda line: json.loads(line[1]))

    # Solution 1.1
    s11 = parsed.flatMap(lambda r: ((r['Origin'], 1), (r['Dest'], 1))) \
                .reduceByKey(lambda a, b: a + b) \
                .updateStateByKey(lambda n, o: sum(n) + (o or 0)) \
                .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)) \
                .foreachRDD(s11_print_results)

    # Solution 1.2
    s12 = parsed.map(s12_do_map) \
                .reduceByKey(s12_do_reduce) \
                .updateStateByKey(s12_do_update) \
                .transform(lambda rdd: rdd.sortBy(lambda x: x[1][0]['AvgDelay'], ascending=True)) \
                .foreachRDD(s12_print_results)

    # Solution 1.3
    s13 = parsed.map(s13_do_map) \
                .reduceByKey(s12_do_reduce)\
                .updateStateByKey(s12_do_update) \
                .transform(lambda rdd: rdd.sortBy(lambda x: x[1][0]['AvgDelay'], ascending=True)) \
                .foreachRDD(s13_print_results)

    ssc.start()
    ssc.awaitTermination()
