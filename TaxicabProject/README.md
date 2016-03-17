# Taxicab Demographics
This is a little project I did looking at how demographics correlate with yellow cab drop offs in NYC.
If you want a non-technical introduction, the best spot would be the
[blog post](https://www.springboard.com/blog/do-rich-people-take-more-taxis/) I wrote for Springboard.
The [technical write up](https://github.com/ThomasProctor/Slide-Rule-Data-Intensive/blob/master/TaxicabProject/TechnicalWriteUp/WriteUp.pdf)
contains the more technical details, as well as a description of the code contained in the [Code directory](https://github.com/ThomasProctor/Slide-Rule-Data-Intensive/tree/master/TaxicabProject/Code).

I used [data from the NYC TLC for 2013](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml).
The demographic data comes from the census bureau.
I got my data from [the nice nhgis website](https://nhgis.org/).
Shapefiles for doing the GIS work can be found from the [NYC city government](http://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page)
or from the [census bureau](https://www.census.gov/geo/maps-data/data/tiger-line.html).

Unfortunately, I was learning SQL and PostGIS while I was creating my SQL database, so I didn't document how to create it.
If you want to reproduce what I did, you'll have to gather the data yourself and build the SQL database on your own. :(

Special thanks to Eric Rynerson and Christos Papadopoulos for all their advice on the direction of this project, and to Sandra Voss and Roger Huang for editing my writing.
