'''
The full table of lon,lat points -> FIPS codes can be pretty memory intensive.
This gets all the relevent information in about a quarter of the memory.
'''

import numpy as np
import pandas as pd
from os import path
#
tractdictpath=path.expanduser('~/Documents/TaxiTripData/lonlatlookup.csv')
indextractpath=path.expanduser('~/Documents/TaxiTripData/indextractlookup.csv')
fipsindexpath=path.expanduser('~/Documents/TaxiTripData/fipscodesindex.csv')
#
chk=int(1e7)
tractdictit=pd.read_csv(tractdictpath,chunksize=chk,iterator=True,header=None,\
names=['longitude','latitude','fipscodes'],delimiter='\t',na_values='\N')
# Since I'm only using the ~2000 census tracts in NYC, I don't need to use the big huge FIPS codes, and can just assign a 16 bit integer to each of them.
tractints=pd.read_csv(fipsindexpath,names=['fipsindex','fipscodes'],\
dtype={'fipsindex':np.uint16,'fipscodes':np.float64}).set_index('fipscodes')
tractdict=pd.Series([],name='fipscodes')
#convert gps coordinates to an single integer (can be small as a uint32 if not index)
lonmax=-73.6968
latmin=40.4926
lonzero=int(-lonmax*10000)
latzero=int(latmin*10000)
for i in tractdictit:
	i=i.dropna().join(tractints,on='fipscodes')
	i['gpsindex']=(((-i['longitude']*10000-lonzero)*10000)+(i['latitude']*10000-latzero)).astype(np.intp)        
	tractdict=tractdict.append(i.dropna().set_index('gpsindex')['fipsindex'].astype(np.int16))
tractdict.to_csv(indextractpath,index=True,index_label='gpsindex',header=True)
