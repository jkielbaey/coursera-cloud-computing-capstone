#!/usr/bin/python

import re, json, os
import boto3
import logging

s3client = boto3.client('s3')
lambda_client = boto3.client('lambda')

logger = logging.getLogger()

if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)

def handler(event, context):
    """
        event should contain:
        - src-bucketname
        - src-prefix
        - dst-bucketname
        - dst-prefix
    """
    logger.info('Invoking handler.')

    resp = s3client.list_objects_v2(
        Bucket=event['src-bucketname'],
        Prefix=event['src-prefix']
    )

    if resp['KeyCount'] == 0:
        logger.warn('No files found.')
    else:
        logger.info('Files found: %s' % resp['KeyCount'])
        nr_failed = 0
        nr_success = 0
        for f in resp['Contents']:
            if re.search('.zip$', f['Key']):
                logger.info('Found file %s.' % f['Key'])
                args = {
                    'src-bucketname': event['src-bucketname'],
                    'key': f['Key'],
                    'dst-bucketname': event['dst-bucketname'],
                    'dst-key-prefix': event['dst-prefix']
                }
                logger.info('Starting async processing of %s...' % f['Key'])
                results = lambda_client.invoke_async(
                    FunctionName='capstone-cleaning-dev-handle_zipfile',
                    InvokeArgs=json.dumps(args)
                )
                if results['Status'] == 202:
                    logger.info('Lambda invoked successfully.')
                    nr_success += 1
                else:
                    logger.error('Failed to start lambda for %s.' % f['Key'])
                    nr_failed += 1
        logger.info('Processing of ZIP files started.')
        logger.info('%d lambda started successfully' % nr_success)
        logger.info('%d lambda failed to start.' % nr_failed)
