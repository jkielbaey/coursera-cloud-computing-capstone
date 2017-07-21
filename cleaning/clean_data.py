#!/usr/bin/python

import os, re, json, csv
from multiprocessing import Pool

 # Year, Month, DayOfMonth, DayOfWeek, Quarter, UniqueCarrier, FlightNumber, Origin, Dest, CRSDepTime, DepTime, DepDelayMinutes, CRSArrTime, ArrTime, ArrDelayMinutes, Cancelled
SELECTED_FIELDS = [0, 2, 3, 4, 1, 6, 10, 11, 17, 23, 24, 26, 34, 35, 37, 41]

INPUT_DIRECTORY  = '/Volumes/Riemann_HDD/Users/johan/No_Backup/capstone/data/airline_ontime/1988/'
OUTPUT_DIRECTORY = '/tmp/capstone_data'
def handle_csvfile(csvfile):
    csvfh_in = open(csvfile, 'r')
    header = None
    output_data = {}

    csvreader = csv.reader(csvfh_in)
    for line in csvreader:
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
    csvfh_in.close()

    write_new_csvfiles(OUTPUT_DIRECTORY, output_data, header)
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
            if re.search('[0-9].csv$', filename):
                csvfiles.append(os.path.join(path, filename))
    return csvfiles

if __name__ == '__main__':
    csvfiles = find_csvfiles_in_source(INPUT_DIRECTORY)

    print "Identified CSV files:"
    print json.dumps(csvfiles, indent=2)

    # print "\n"
    # print "Handling 1 file:"
    # print str(handle_csvfile(csvfiles[0]))

    print "\n"
    print "Handling CSV files:"
    p = Pool(4)
    res = p.map(handle_csvfile, csvfiles)
    print json.dumps(res, indent=4)
