'''Documentation

1.) Create an RDD and SQL DataFrame of the Trip Data. 
2.) Create an RDD and SQL DataFrame of the Location Codes
3.) Calculation the frequency of trips by Pick-up/Drop-off groupings. 
4.) Convert the Pikc-up / Drop-off locations to the Zone Labels 

'''

# CREATE SPARK Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.getOrCreate()


# CREATE A DATAFRAME OF THE TAXI TRIP DATA
File_TripData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/yellow_tripdata_2017-02.csv'
df_TripData = my_spark.read.csv(File_TripData, header = True)


# CREATE A DATAFRAME OF THE LOCATION CODES
File_ZoneData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/taxi+_zone_lookup.csv'
df_Loc_Codes = my_spark.read.csv(File_ZoneData, header = True) 


# Limit DataFrame to Pick-Up And Drop-Off Locations
PU_DO_LocationCols = df_TripData.select(df_TripData['PULocationID'],df_TripData['DOLocationID'])

# Convert Limited DataFrame to an RDD
PU_DO_RDD = PU_DO_LocationCols.rdd.map(tuple)

# Calculate Count of Trips
PU_DO_Frequency = PU_DO_RDD.map(lambda x: ((x[0], x[1]),1)).reduceByKey(lambda x,y: x+y).persist()

# Get Top 10 Most Frequency Routes
PU_DO_Freq_Top10 = PU_DO_Frequency.map(lambda x: (x[1], x[0])).sortByKey(ascending = False).take(10)
print('#########', PU_DO_Freq_Top10)

# Limit Location RDD to LocationID / Borough
df_locID_Zone = df_Loc_Codes.select(df_Loc_Codes['LocationID'], df_Loc_Codes['Zone']).collect()


'''Key to Variables
ID_loc[0]	= LocationID 
ID_loc[1]	= Zone
ID_trip[0] 	= Count for the pair PU/DO location
ID_trip[1][0]	= Pickup location
ID_trip[1][1] 	= Drop off location 
'''


# Iterate over the list of all LocationID's
def get_match_ID_Borough(Trip_RDD, Loc_RDD):

	for ID_trip in Trip_RDD:
	
		# Define Variables to returnto user.  Reset on each iteration
		PU_loc = ''
		DO_loc = ''
	
		# Iterate over our list of Top10 Trips
		for ID_loc in Loc_RDD:
			
			# Check to see if we can match the Pick-up location
			if ID_trip[1][0] == ID_loc[0]:
				# if match, set to Borough name
				PU_loc = str(ID_loc[1]) 
			# if not, return the original value

			# Check to see if we can match the Drop-off location
			if ID_trip[1][1] == ID_loc[0]:
				# if match, set to Borough name
				DO_loc = ID_loc[1]
	
		print(('Count => ', ID_trip[0], (PU_loc, DO_loc)))	


get_match_ID_Borough(PU_DO_Freq_Top10, df_locID_Zone)



