



from __future__ import division
import numpy as np
#import sqlalchemy as sql
import pandas as pd
from os import path
import math as m
from datetime import datetime
#from random import random
#from datetime import datetime
#from sys import getsizeof
#from cPickle import 




cnksize=int(55e5)




nsweeps=60
ntracts=2168

print('%i sweeps' % (nsweeps))




datapath=path.expanduser('~/Documents/TaxiTripData/')


# Import list of tracts, and take the subset of it that we want.



n_tracts_sweep=int(m.floor(ntracts/nsweeps))
print('%i tracts in each sweep' % (n_tracts_sweep))




from glob import glob

newpath=glob(datapath+'2013taxi_trip_data/yellow_tripdata*.csv')

oldpatha=glob(datapath+'2013taxi_trip_data/trip_data_*.csv.zip.gz')

oldpatha.sort()

oldpathb=[oldpatha.pop(1) for i in xrange(3)]




def diffSeries(fips,seri):
    a=seri.sort_values(inplace=False).diff().describe()
    a.name=int(fips)
    return a


# Hard work of 

def tract_concat_rawdf(df,tractdict):
    latmin=40.4926
    lonmax=-73.6968
    # Convert lon,lat to the single integer form that I use for the tractdict
    df['gpsindex']=(((lonmax-df['longitude'].round(decimals=4))*10000*10000)+(df['latitude'].round(decimals=4)-latmin)*10000).dropna().astype(np.int32)
    #df.set_index('gpsindex',inplace=True)
    df= df.join(tractdict,how='inner',on='gpsindex')[['dropoff_datetime',tractdict.name]]
    return df.set_index(np.arange(df.shape[0]))#[['dropoff_datetime',tractdict.name]]
    #return df['dropoff_datetime'].groupby(df[tractdict.name],sort=False,squeeze=True)
    




#currentsweep=0




for currentsweep in range(nsweeps):
#for currentsweep in [2160]:
	print("sweep number %i" % (currentsweep))
	print(str(datetime.now()))

	

	#currenttracts=(pd.read_csv(datapath+'fipspermutation.csv',header=None)[0][currentsweep*n_tracts_sweep:(currentsweep+1)*n_tracts_sweep].tolist())
	
	currenttracts=(pd.read_csv(datapath+'fipspermutation.csv',header=None)[0][currentsweep:].tolist())
	print(currenttracts)

	# Import lonlat -> tract dictionary, and select out just the subset of tracts we're searching for this sweep

	

	lonlat_to_tract=pd.read_csv(datapath+'indextractlookup.csv',header=1,names=['gpsindex','fipsindex'],dtype={'gpsindex':np.intp,'fipsindex':np.uint16},index_col=0                           )['fipsindex']


	

	lonlat_to_tract=lonlat_to_tract[lonlat_to_tract.isin(currenttracts)]


	# Create a list of all the data files

	

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

	

	tract_datetime=pd.DataFrame([],columns=['dropoff_datetime','fipsindex'])


	
	print("starting hard work at %s" % (str(datetime.now())))
	for iterator in iteratorlist:
		tract_datetime=pd.concat([tract_datetime] + [tract_concat_rawdf(chunk,lonlat_to_tract) for chunk in iterator],
							 ignore_index=True,copy=False)
		print("next month at %s" % (str(datetime.now())))


	
	
	print("grouping at %s" % (str(datetime.now())))

	tract_datetime=pd.concat([diffSeries(name,group) for name,group in tract_datetime.dropoff_datetime.groupby(tract_datetime.fipsindex)],axis=1).transpose()


	
	#print(tract_datetime)
	print('writing at %s' % (str(datetime.now())))
	#with open(datapath+"2013taxi_trip_data/tractstats.csv",'a') as f:
	#    a.to_csv(datapath+"2013taxi_trip_data/tractstats.csv",mode='a',header=False,index=True)
	tract_datetime.to_csv(datapath+"2013taxi_trip_data/tractstats.csv",mode='a',header=False,index=True)


	

	#pd.read_csv(datapath+"2013taxi_trip_data/tractstats.csv",index_col=0,parse_dates=True)['mean']


	

	#pd.to_timedelta(pd.read_csv(datapath+"2013taxi_trip_data/tractstats.csv",index_col=0,parse_dates=True)['mean'])


	

	#tract_datetime=tract_datetime.append(tract_concat_rawdf(chunk,lonlat_to_tract),ignore_index=True)


	



