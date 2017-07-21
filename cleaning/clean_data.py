#!/usr/bin/python

import os, re, json, csv, sys
import subprocess
from multiprocessing import Pool
import multiprocessing

 # Year, Month, DayOfMonth, DayOfWeek, Quarter, UniqueCarrier, FlightNumber, Origin, Dest, CRSDepTime, DepTime, DepDelayMinutes, CRSArrTime, ArrTime, ArrDelayMinutes, Cancelled
SELECTED_FIELDS = [0, 2, 3, 4, 1, 6, 10, 11, 17, 23, 24, 26, 34, 35, 37, 41]

NR_CPUS = multiprocessing.cpu_count()

INPUT_DIRECTORY  = '/data/RAW'
#INPUT_DIRECTORY  = '/Volumes/Riemann_HDD/Users/johan/No_Backup/capstone/data/airline_ontime/1988'

UNZIP_DIRECTORY  = '/data/UNZIP'
#UNZIP_DIRECTORY  = '/tmp/capstone_unzip'

OUTPUT_DIRECTORY = '/data/CLEANED'
#OUTPUT_DIRECTORY = '/tmp/capstone_data'

def handle_csvfile(csvfile):
    csvfh_in = open(csvfile, 'r')
    header = None
    output_data = {}

    csvreader = csv.reader(csvfh_in)

    for line in csvreader:
        try:
            output_line = [ line[i] for i in SELECTED_FIELDS ]
            if header is None:
                header = output_line
                # Add header of extra computed fields.
                header.append('DepOnTime')
                header.append('ArrOnTime')
            else:
                year = int(line[0])
                month = int(line[2])
                day = int(line[3])
                # Add value to indicate if flight departed/arrived on time.
                output_line.append( 1 if line[26] == '0.00' else 0 )
                #print 'DEP: ' + str(line[23:27]) + ' = ' + str(output_line[-1])
                output_line.append( 1 if line[37] == '0.00' else 0 )
                #print 'ARR: ' + str(line[33:37]) + ' = ' + str(output_line[-1])
                if year not in output_data:
                    output_data[year] = {}
                if month not in output_data[year]:
                    output_data[year][month] = {}
                if day not in output_data[year][month]:
                    output_data[year][month][day] = []
                output_data[year][month][day].insert(0, output_line)
        except IndexError as e:
            print "Error processing line %s in %s. (%s)" % ('|'.join(line), csvfile, str(e))
    csvfh_in.close()

    write_new_csvfiles(OUTPUT_DIRECTORY, output_data, header)
    print "File %s done." % csvfile
    return csvfile

def write_new_csvfiles(output_directory, output_data, header):
    for year in output_data.keys():
        year_dir = os.path.join(output_directory, '%04d' % year)
        if not os.path.exists(year_dir): os.mkdir(year_dir)

        for month in output_data[year].keys():
            month_dir = os.path.join(year_dir, '%02d' % month)
            if not os.path.exists(month_dir): os.mkdir(month_dir)

            for day in output_data[year][month]:
                output_file = os.path.join(
                    month_dir,
                    '%04d_%02d_%02d.csv' % (year, month, day)
                )
                fh = open(output_file, 'w')
                csvwriter = csv.writer(fh)
                csvwriter.writerow(header)
                csvwriter.writerows(output_data[year][month][day])
                fh.close()

def find_csvfiles_in_source(src_dir):
    csvfiles = []
    for path,dirs,files in os.walk(src_dir):
        for filename in files:
            if re.search('.csv$', filename):
                csvfiles.append(os.path.join(path, filename))
    return csvfiles

def extract_zipfile(zipfile):
    m = re.search('/([^/]+).zip$', zipfile)
    extract_dir = os.path.join(UNZIP_DIRECTORY, m.group(1))
    if not os.path.exists(extract_dir): os.mkdir(extract_dir)
    try:
        subprocess.check_call(['unzip', '-f', '-o', '-qq', '-d', extract_dir, zipfile])
        return { 'zipfile': zipfile, 'status': 'OK' }
    except subprocess.CalledProcessError as e:
        return { 'zipfile': zipfile, 'status': 'ERROR', 'errormsg': str(e) }

def find_zipfiles_in_source(src_dir):
    csvfiles = []
    for path,dirs,files in os.walk(src_dir):
        for filename in files:
            if re.search('.zip$', filename):
                csvfiles.append(os.path.join(path, filename))
    return csvfiles

def process_results_extract(results):
    n_failed = 0
    n_success = 0

    print "Extract failed:"
    for f in results:
        if f['status'] == 'OK':
            n_success += 1
        else:
            print "  - %s" % f['zipfile']
            n_failed += 1

    print "Total files: %d" % (n_failed + n_success)
    print "Nr failed: %d // Nr success: %d" % (n_failed, n_success)

if __name__ == '__main__':
    # Unzip files
    print "Locating ZIP files..."
    zipfiles = find_zipfiles_in_source(INPUT_DIRECTORY)
    print "  %d files found." % len(zipfiles)
    print "\n"
    print "Extracting ZIP files..."
    p = Pool(NR_CPUS)
    results = p.map(extract_zipfile, zipfiles)

    process_results_extract(results)

    print json.dumps(sorted(zipfiles), indent=2)
    # Process resulting CSV files.
    print "Locating CSV files..."
    csvfiles = find_csvfiles_in_source(UNZIP_DIRECTORY)
    print "  %d files found." % len(csvfiles)

    print "Identified CSV files:"
    print json.dumps(sorted(csvfiles), indent=2)

    print "\n"
    print "Handling CSV files:"
    p = Pool(NR_CPUS)
    res = p.map(handle_csvfile, csvfiles)
    print json.dumps(res, indent=4)
