
'''
This creates a table that contains each longitude,latitude point in a rectangle
covering the NYC area and the FIPS code for the census tract that contains that
point.
'''



import pandas as pd
import sqlalchemy as sqla
import numpy as np




latmin=40.4926
latmax=40.9206
lonmax=-73.6968
lonmin=-74.2598




lats=pd.Series(np.arange(latmin,latmax+0.00001,0.0001)).astype(str)
lons=pd.Series(np.arange(lonmin,lonmax,0.0001)).astype(str)




engine=sqla.create_engine('postgresql://postgres:postgres@localhost:5432/TaxiData',echo=False)




s=sqla.text("CREATE TABLE gpstractdict (fipscodes varchar) ")
conn=engine.connect()
conn.execute(s)




s=sqla.text("SELECT AddGeometryColumn ('gpstractdict','gpspoint',4269,'POINT',2)")
conn.execute(s)




s=sqla.text("ALTER TABLE gpstractdict ADD PRIMARY KEY (gpspoint)")
conn.execute(s)


# Not enough memory to keep insert string for all points, have to break it up.




for i in lats:
    s=sqla.text("INSERT INTO gpstractdict (gpspoint) VALUES (ST_SetSRID(ST_Point("+    (","+i+"),4269)), (ST_SetSRID(ST_Point(").join(lons)+","+i+"),4269))")
    conn.execute(s)
    


# nyctracts contains all the shapes of the census tracts for NYC, built from the individual county files provided by the Census Bureau. 

# SQL code actually matches our GPS points to census tracts, all the work here is done by the SQL server not python

s="UPDATE gpstractdict SET fipscodes = nyctracts.geoid10 FROM nyctracts WHERE ST_Contains(nyctracts.geom, gpstractdict.gpspoint)"




s=sqla.text(s)
conn.execute(s)






