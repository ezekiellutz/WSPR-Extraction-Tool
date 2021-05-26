
#### WSPR Extraction Tool Version 2.0.0 ###

# -*- coding: utf-8 -*-

import datetime
import time
import gzip
import pandas as pd
import numpy as np

# Inputs
IN_FILE_NAME = 'raw_data.csv.gz'
OUT_FILE_NAME = 'sorted_wsprspots.csv'
CALLSIGN = input('Enter the callsign you want to look for: \t')
print ("Searching for: ", CALLSIGN)
print ("\nPlease wait while the extraction process begins...")

# Make a generator object here
def generate_filter(file_name):
    with gzip.open(file_name, 'rt') as file:
        for row in file:
             if CALLSIGN in row:
                yield row # 'yield' connects 'row' with the 'next' operator

# pull data out of generator
def create_filtered_dataframe(gen_obj):
    data = []
    try:
        # use the generator to iterate through each row in the file
        for row in gen_obj:
            # 'split' splits the values in the row at each comma.
            data.append(row.split(','))
        return pd.DataFrame(data)
    except StopIteration: # stop when end of file is reached
        return pd.DataFrame(data)

def main(in_file_name, out_file_name):
    header_names = ['Spot ID', 'Timestamp', 'Reporter', "Reporter's Grid",
                    'SNR', 'Frequency', 'Call Sign', 'Grid', 'Power',
                    'Drift', 'Distance', 'Azimuth', 'Band', 'Version', 'Mode']
    data_types = {'Spot ID':np.uint32, 'Timestamp':np.uint32, 'Reporter':str,
                  "Reporter's Grid":str, 'SNR':np.int8, 'Frequency':np.float32,
                  'Call Sign':str, 'Grid':str, 'Power':np.int8, 'Drift':np.int8,
                  'Distance':np.uint16, 'Azimuth':np.uint16, 'Band':np.int16,
                  'Version':str, 'Mode':np.uint8}
    t_start = time.time() #start time clock for duration measurments

    filt_gen_obj = generate_filter(in_file_name) # create generator object
    data = create_filtered_dataframe(filt_gen_obj) # collect filtered data
    if data.empty is True:
        print('Filtered values not found in file')
        t_search = time.time()-t_start
        print('Seconds to search file: ', round(t_search, 2))
        return
    data.columns = header_names # add column headers
    data = data.astype(data_types) # set data types

    # convert timestamp from EPOCH time to datetime string
    data['Timestamp'] = data['Timestamp'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))

    t_read = time.time()-t_start # record the elapsed time for the .csv import
    print('Seconds to read file: ', round(t_read, 2))

    # write file to .csv
    data.to_csv(out_file_name)
    t_write = time.time()-t_read -t_start # elapsed time for the .csv export
    print('Seconds to write file: ', round(t_write, 2))

if __name__ == "__main__":
    main(IN_FILE_NAME, OUT_FILE_NAME)

print ("The extraction process is now complete!")
'''
#### REVISION HISTORY ####

Version 1.0.0  
    Created on Sat Apr 11 11:05:05 2020
    @author: Jordan Lewis

Version 2.0.0
    Created on Tues Apr 27 10:31:00 2021
    @author: Ezekiel Lutz (KD9OWT)

'''