#!/usr/bin/python
import re
import json
import csv
import io
import os
from zipfile import ZipFile
import boto3
import cStringIO
import logging

SELECTED_FIELDS = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier', 'FlightNum', 'Origin', 'Dest',
    'CRSDepTime', 'DepTime', 'DepDelay', 'DepDelayMinutes', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes',
    'Cancelled'
]

s3client = boto3.client('s3')

logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)


def write_new_csvfiles(dest_bucket, dest_prefix, data):
    """
        Will write the processed data as object in an S3 bucket.
    """

    results = {
        'written_lines': 0,
        'written_files': []
    }
    for year, year_data in data['data'].iteritems():
        for month, month_data in year_data.iteritems():
            for day, day_data in month_data.iteritems():
                output_file = "%s/%04d/%02d/%04d_%02d_%02d.csv" % (
                    dest_prefix,
                    year,
                    month,
                    year,
                    month,
                    day
                )
                output = cStringIO.StringIO()
                writer = csv.DictWriter(output, fieldnames=day_data[0].keys())
                writer.writeheader()
                writer.writerows(day_data)
                results['written_lines'] += len(day_data)

                logging.debug('Putting CSV file %s onto S3.' % output_file)
                s3client.put_object(
                    Bucket=dest_bucket,
                    Key=output_file,
                    Body=output.getvalue(),
                    ContentType='text/csv'
                )
                output.close()
                results['written_files'].append({
                    'bucketname': dest_bucket,
                    'key': output_file
                })
                logging.info('Data file %s written to S3 (lines: %d).' % (output_file, len(day_data)))
    return results


def process_csvfile(csvfile, csvlines):
    """
        Parses contents of a csvfile, selects a number of
        fields and add some aggregated fields.
        Returns a dict containing the files to be written
        and uploaded into S3. Content of dict is grouped by year, month and day.
        {
            'YYYY': {
                'MM': {
                    'DD': {
                        { 'fieldname1': 'field1', 'fieldname2': 'field2', ... },
                        ...
                    }
                }
            }
        }
    """
    logging.info("Processing CSV file %s." % csvfile)
    output_data = {}

    csvreader = csv.DictReader(csvlines)

    nr_lines = 0
    skipped_lines = 0
    logging.debug('Fieldnames: %s' % json.dumps(sorted(csvreader.fieldnames)))
    for line in csvreader:
        try:
            output_line = {i: line[i] for i in SELECTED_FIELDS}
            year = int(line['Year'])
            month = int(line['Month'])
            day = int(line['DayofMonth'])

            # # Add value to indicate if flight departed/arrived on time.
            if year not in output_data:
                output_data[year] = {}
            if month not in output_data[year]:
                output_data[year][month] = {}
            if day not in output_data[year][month]:
                output_data[year][month][day] = []
            output_data[year][month][day].append(output_line)
            nr_lines += 1
        except IndexError as e:
            logging.error("Error processing line %s in %s. (%s)" % ('|'.join(line), csvfile, str(e)))
    logging.info("Processed %d lines from %s." % (nr_lines, csvfile))
    return {
        'csvfile': csvfile,
        'processed_nr_lines': nr_lines,
        'skipped_nr_lines': skipped_lines,
        'data': output_data
    }


def download_and_extract_zipfile(source_bucket, source_key):
    """
        Downloads a zipfile from S3 and returns a list of csv file(s)
        containing in the zipfile.
        Each item in the list is a dict containing:
            - filename
            - content of the csv file (as list of lines)
            - number of lines in the csv file.
    """

    logging.info('Downloading object %s/%s from S3.' % (source_bucket, source_key))
    image = s3client.get_object(
        Bucket=source_bucket,
        Key=source_key
    )

    csvfiles = []
    logging.info('Opening and retrieving CSV files from %s.' % source_key)
    tf = io.BytesIO(image["Body"].read())
    tf.seek(0)
    zipfile = ZipFile(tf, mode='r')
    for file in zipfile.namelist():
        if re.search('.csv$', file):
            logging.debug('CSV file %s found in object.' % file)
            lines = zipfile.open(file).readlines()
            item = {
                'filename': file,
                'content': lines,
                'nr_lines': len(lines)-1
            }
            logging.info('CSV file %s read into memory.' % file)
            csvfiles.append(item)
    tf.close()
    zipfile = None
    tf = None
    image = None
    return csvfiles


def handle_zipfile(event, context):
    """
        event is expected as dictionary
        {
            'src-bucketname': <s3-bucket>,
            'key': <location-of-zipfile>,
            'dst-bucketname': <s3-bucket>,
            'dst-key-prefix': <prefix>
        }
    """
    logging.info('Handler invoked for %s/%s' % (event['src-bucketname'], event['key']))
    csvfiles = download_and_extract_zipfile(
        event['src-bucketname'],
        event['key']
    )
    for csvfile in csvfiles:

        logging.info('Processing data from %s.' % csvfile['filename'])
        processed_data = process_csvfile(csvfile['filename'], csvfile['content'])

        logging.info('Writting processed data from %s.' % csvfile['filename'])
        results = write_new_csvfiles(event['dst-bucketname'], event['dst-key-prefix'], processed_data)

        logging.info('Validating written data from %s.' % csvfile['filename'])
        validation_ok = 1
        if csvfile['nr_lines'] != (processed_data['processed_nr_lines'] + processed_data['skipped_nr_lines']):
            logging.error('Not all lines from %s were processed. (read: %d vs processed: %d).' %
                          (csvfile['filename'], csvfile['nr_lines'], processed_data['processed_nr_lines']))
            validation_ok = 0
        else:
            logging.info('All lines read from %s were processed.' % csvfile['filename'])
        if csvfile['nr_lines'] != (results['written_lines'] + processed_data['skipped_nr_lines']):
            logging.error('Not all lines from %s were written to S3. (read: %d vs written: %d).' %
                          (csvfile['filename'], csvfile['nr_lines'], results['written_lines']))
            validation_ok = 0
        else:
            logging.info('All lines read from %s were written to S3 after processing.' % csvfile['filename'])

        if validation_ok == 1:
            logging.info('--- Processing of file %s completed successfully.' % csvfile['filename'])
        else:
            logging.error('+++ Error occured while processing file %s.' % csvfile['filename'])

    return {
        'status': 'OK' if validation_ok == 1 else 'ERROR'
    }
