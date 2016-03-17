'''
Create a index for NYC fipscodes for less memory usage, and create a 
randomized order so that we have even memory usage for chunks of fipscodes
'''


import numpy as np
import sqlalchemy as sql
import pandas as pd
from os import path
from datetime import datetime
#from sys import getsizeof
#from cPickle import 




engine = sql.create_engine('postgresql://postgres:postgres@localhost:5432/TaxiData',echo=False)




fipscodes=pd.read_sql('SELECT fipscodes FROM lotsofdata',engine)




fipsindexpath=path.expanduser('~/Documents/TaxiTripData/fipscodesindex.csv')




fipscodes['fipscodes'].astype(np.int64).to_csv(indextractpath,index=True)


# Random permutation of fipscodes indices, filed so that we know the order for later.



randomperm=np.random.permutation(np.arange(0,2168))




pd.Series(randomperm).to_csv(path.expanduser('~/Documents/TaxiTripData/fipspermutation.csv'),index=False)

