#!/usr/bin/env python3

import boto3
import csv
import json
import re
import os
import logging
from multiprocessing import Pool

import sys
sys.path.insert(0, './lib')
from kafka import KafkaProducer

lambda_client = boto3.client('lambda')
bucket_name = None
kafka_topic = None

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger()

if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


def handler_file(event, context):
    key_name = event['key_name']
    bucket_name = event['bucket_name']
    kafka_topics = event['kafka_topic'].split(",")
    for t in kafka_topics:
        logging.info("Sending data to topic \"%s\"." % t)
    kafka_hosts = os.environ['KAFKA_HOSTS'].split(",")
    logging.info("Started handling %s." % key_name)
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key_name)
    csvlines = obj.get()['Body'].read().decode('utf-8').splitlines()
    csvreader = csv.DictReader(csvlines)
    nr_lines = 0

    producer = KafkaProducer(bootstrap_servers=kafka_hosts)
    nr_topics = len(kafka_topics)
    topic_id = 0
    logging.info("Producer created for %s." % key_name)
    for l in csvreader:
        producer.send(kafka_topics[topic_id], json.dumps(l))
        topic_id += 1
        nr_lines += 1
        if topic_id == nr_topics:
            topic_id = 0
    producer.flush()
    logging.info("Messages produced. Nr of messages: %d." % nr_lines)
    return nr_lines


def handler_load(event, context):
    bucket_name = event['bucket_name']
    key_prefix = event['key_prefix']
    kafka_topic = event['kafka_topic']

    nr_failed = 0
    nr_success = 0

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    for obj in bucket.objects.filter(Prefix=key_prefix):
        if re.search('\.csv$', obj.key):
            logging.info("File added %s" % obj.key)
            args = {
                'bucket_name': bucket_name,
                'key_name': obj.key,
                'kafka_topic': kafka_topic
            }
            logger.info('Starting async processing of %s...' % obj.key)
            results = lambda_client.invoke_async(
                FunctionName='capstone-kafka-ingest-dev-send_file',
                InvokeArgs=json.dumps(args)
            )
            logger.info("Async processing of %s started." % obj.key)
            if results['Status'] == 202:
                logger.info('Lambda invoked successfully.')
                nr_success += 1
            else:
                logger.error('Failed to start lambda for %s.' % obj.key)
                nr_failed += 1
    logger.info('%d lambda started successfully' % nr_success)
    logger.info('%d lambda failed to start.' % nr_failed)


def worker_lambda(key):
    logger.info("Start processing of %s..." % key)
    args = {
        'bucket_name': bucket_name,
        'key_name': key,
        'kafka_topic': kafka_topic
    }
    results = lambda_client.invoke(
        FunctionName='capstone-kafka-ingest-dev-send_file',
        InvocationType='RequestResponse',
        Payload=json.dumps(args))
    logging.info(str(results))
    if results['StatusCode'] == 200:
        logger.info('Lambda completed successfully.')
        return (key, True)
    else:
        logger.error('Failed to start lambda for %s.' % key)
        return (key, False)


if __name__ == '__main__':
    bucket_name, key_prefix, kafka_topic = sys.argv[1:]

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    files_to_process = []
    for obj in bucket.objects.filter(Prefix=key_prefix):
        if re.search('\.csv$', obj.key):
            logger.info("File added %s" % obj.key)
            files_to_process.append(obj.key)
    pool = Pool(100)
    results = pool.map(worker_lambda, files_to_process)

    success = []
    failed = []
    for result in results:
        if result[1]:
            success.append(result[0])
        else:
            failed.append(result[0])

    if len(failed) != 0:
        print "Not all files were processed successfully :("
        print(str(failed))

    print "%d files completed successfully" % len(success)
