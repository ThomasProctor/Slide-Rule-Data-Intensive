"""
This script will count the number of dropoffs that occur in each
census tract. It is made to run using minimal memory and time. It reads
in drop off locations from TLC data files, converts the lat, lon
coordinates into a single 32 bit integer that maps to points on a grid
that covers just the city of NYC. Then it does a join on a pre-computed
table that maps grid points -> census tracts. Since those census tracts
have been labeled using a 16 bit integer, it then converts that 16 bit
integer into the FIPS code that the census bureau uses.

The output is a count of the number of drop offs in each census tract,
along with statistics about the time between drop offs as a sanity
check on the data.
"""


from __future__ import division
import numpy as np
import pandas as pd
from os import path
import math as m
from datetime import datetime
from glob import glob


# Constants for numerics
cnksize = int(55e5)
nsweeps = 60
ntracts = 2168
# really should be using the logging module.
print('%i sweeps' % (nsweeps))
datapath = path.expanduser('~/Documents/TaxiTripData/')


# Import list of tracts, and take the subset of it that we want.
n_tracts_sweep = int(m.floor(ntracts/nsweeps))
print('%i tracts in each sweep' % (n_tracts_sweep))


# Annoyingly, the TLC changed the naming convention on their data files.
newpath = glob(datapath+'2013taxi_trip_data/yellow_tripdata*.csv')
oldpatha = glob(datapath+'2013taxi_trip_data/trip_data_*.csv.zip.gz')
oldpatha.sort()
oldpathb = [oldpatha.pop(1) for i in xrange(3)]




def diffSeries(fips, seri):
    """computes the count of drop offs, as well as stats about the time between them"""
    a = seri.sort_values(inplace=False).diff().describe()
    a.name = int(fips)
    return a


def tract_concat_rawdf(df, tractdict):
    """Assigns tract to each drop-off via inner join."""
    # Boundaries of NYC
    latmin = 40.4926
    # Since I need to convert lon to positive, actually use the max val.
    lonmax = -73.6968
    # Convert lon,lat to the single integer form that I use for the tractdict
    # This integer stores only 4 relevent decimal places of lat lon.
    # lon is stored in the 100000 - 100000000 places, and lat in 0 - 10000.
    df['gpsindex'] = (((lonmax-df['longitude'].round(decimals=4))*10000*10000) + (df['latitude'].round(decimals=4)-latmin)*10000).dropna().astype(np.int32)
    df = df.join(tractdict, how='inner', on='gpsindex')[['dropoff_datetime', tractdict.name]]
    return df.set_index(np.arange(df.shape[0]))


# In order to save memory, I don't do all census tracts at once. This trades
# time for memory, as I load all the data each time and throw out everything
# that doesn't merge with the tracts that are in the chunk of census tracts
# I focus on in each loop.
for currentsweep in range(nsweeps):
	print("sweep number %i" % (currentsweep))
	print(str(datetime.now()))
	currenttracts=(pd.read_csv(datapath+'fipspermutation.csv',header=None)[0][currentsweep:].tolist())
	print(currenttracts)
	# Import lonlat -> tract dictionary, and select out just the subset of tracts we're searching for this sweep
	lonlat_to_tract=pd.read_csv(datapath+'indextractlookup.csv',header=1,names=['gpsindex','fipsindex'],dtype={'gpsindex':np.intp,'fipsindex':np.uint16},index_col=0                           )['fipsindex']
	lonlat_to_tract=lonlat_to_tract[lonlat_to_tract.isin(currenttracts)]
	# Create a list of all the data. Note that each list element is a
        # a generator that reads in a chunk of the csv file, (iterator=True option)
        # so this is actually lazy, and doesn't actually hold anything in
        # memory until it is accessed.
	iteratorlist=[pd.read_csv(i,iterator=True,chunksize=cnksize,usecols=[6,12,13],parse_dates=['dropoff_datetime'],
						 dtype={u'longitude':np.float_, u'latitude':np.float_},
						 names=[u'dropoff_datetime', u'longitude', u'latitude'],header=0) for i in oldpatha]+ \
				  [pd.read_csv(i,iterator=True,chunksize=cnksize,usecols=[2,9,10],parse_dates=['dropoff_datetime'],
						 dtype={u'longitude':np.float_, u'latitude':np.float_},
						 names=[u'dropoff_datetime', u'longitude', u'latitude'],header=0) for i in newpath]+ \
				  [pd.read_csv(i,iterator=True,chunksize=cnksize,usecols=[6,12,13],parse_dates=['dropoff_datetime'],
						 dtype={u'longitude':np.float_, u'latitude':np.float_},
						 names=[u'dropoff_datetime', u'longitude', u'latitude'],header=0) for i in oldpathb]
	# Go through each file, selecting out the ones in our subset
	tract_datetime = pd.DataFrame([], columns=['dropoff_datetime','fipsindex'])
	print("starting hard work at %s" % (str(datetime.now())))
	for iterator in iteratorlist:
                # Since lat_lon_tract only contains the subset of census tracts
                # we're focusing on, the call to tract_concat_rawdf will throw
                # out drop offs in anly other census tract.
		tract_datetime = pd.concat([tract_datetime] + [tract_concat_rawdf(chunk, lonlat_to_tract)
                                                               for chunk in iterator],
                                           ignore_index=True, copy=False)
		print("next month at %s" % (str(datetime.now())))
	print("grouping at %s" % (str(datetime.now())))
	tract_datetime = pd.concat([diffSeries(name, group)
                                    for name, group
                                    in tract_datetime.dropoff_datetime.groupby(tract_datetime.fipsindex)], axis=1).transpose()
	print('writing at %s' % (str(datetime.now())))
	tract_datetime.to_csv(datapath+"2013taxi_trip_data/tractstats.csv",mode='a', header=False, index=True)
