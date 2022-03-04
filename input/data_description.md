# Data Description
## `cab_rides.csv`
* column header: `distance,cab_type,time_stamp,destination,source,price,surge_multiplier,id,product_id,name`
* ex record: `0.44,Lyft,1544952607890,North Station,Haymarket Square,5,1,424553bb-7174-41ea-aeb4-fe06d4f4b9d7,lyft_line,Shared`
* column types: float,string,long,string,string,double,double,string,string,string
* used attributes: `distance,cab_type,time_stamp,destination,source,price,surge_multiplier,name`
  * i.e. just ignore the two ids
## `weather.csv`
* column header: `temp,location,clouds,pressure,rain,time_stamp,humidity,wind`
  * use all attributes
* ex record: `42.42,Back Bay,1,1012.14,0.1228,1545003901,0.77,11.25`
* column types: float,string,int,float,float,long,float,float
# Manipulation Details
* inner join ride and weather data on the `time_stamp` attribute in both data files
  * inner join so only records with a matching timestamp record are kept
* timestamp: exact timestamps do NOT match up, so we need to simplify the data before joining.
  * --> have the "category" be an hour of the day (use military 24hr time)
  * Q: what do the integers in the timestamp mean? Date? Time?
    * A: these are epoch times (https://www.epochconverter.com/)
  * Q: when bucketing timestamps, how do we handle multimatch timestamps, for example, a ride in the hour of 11am with 5 different weather records, or vice versa?
    * --> could keep all but the seconds time data in the timestamp --> does that yield enough matches or do we need to remove more detail? or, does that still yield collisions (i.e. are there multiple records still in the same minute?)
* other than timestamp, we shouldn't need to convert any other columns from numeric to categorical because the distance measures are actually perfectly made for numeric data.
* numeric data standardization: standardize the numeric data ranges because some are on the 0.1 scale while others are on the 1000 scale
