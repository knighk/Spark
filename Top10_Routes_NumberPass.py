'''Documentation

1.) Create an RDD and SQL DataFrame of the Trip Data. 
2.) Create an RDD and SQL DataFrame of the Location Codes
3.) Calculation the frequency of trips by Pick-up/Drop-off groupings. 
4.) Convert the Pikc-up / Drop-off locations to the Zone Labels 

'''



# CREATE SPARK Session
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.master('local[4]').getOrCreate()

# IMPORT PYTHON PACKAGES
import pandas as pd
import os



# CREATE A DATAFRAME OF THE TAXI TRIP DATA
File_TripData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/yellow_tripdata_2017-02.csv'
df_TripData = my_spark.read.csv(File_TripData, header = True)


# CREATE A DATAFRAME OF THE LOCATION CODES
File_ZoneData = r'/home/ccirelli2/Desktop/Scalable_Analytics/Final_Project/taxi+_zone_lookup.csv'
df_Loc_Codes = my_spark.read.csv(File_ZoneData, header = True) 


# Limit DataFrame to Number of Passengers, Pick-Up And Drop-Off Locations
PU_DO_NumPass = df_TripData.select(df_TripData['passenger_count'], df_TripData['PULocationID'],df_TripData['DOLocationID'])

# Convert Limited DataFrame to an RDD
PU_DO_NumPass_RDD = PU_DO_NumPass.rdd.map(tuple)

# Transform RDD Such that the Key is the Num Passengers, Value = PU/DO
'''Documentation
- [0] = Number of passengers, [1] = Pick-up location, [2] = Drop-of location
- Enumerate tuple => ((NumPass, PU, DO), 1)
- ReduceByKey => ((NumPass, PU, DO), Count)
- Flip structure => Count now key. 
- Persist => As we will use this RDD to calculate each of the NumPass categories. 
'''

KeyValue_RDD = PU_DO_NumPass_RDD.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], (x[0]))).persist()


'''****Note that there are a number of partition values with empty nodes'''


# Get Top 5 By Number of Passengers
'''Documentation
- Filter =>  [1][0] is now the number of passengers.  Set this to 1-n 
- Persist => For purposes of running on the cluster we should persist the RDD at this stage. 
- Sort => Sort by the count and take the top 10.
- Map => Reorder such that the key is our Num of Passengers, PU, DO 
'''

Top_PUDO_PassNum_1 = KeyValue_RDD.filter(lambda x: x[1][0] == '1'
                     ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)
Top_PUDO_PassNum_2 = KeyValue_RDD.filter(lambda x: x[1][0] == '2'                                                   ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)
Top_PUDO_PassNum_3 = KeyValue_RDD.filter(lambda x: x[1][0] == '3'                                                   ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)
Top_PUDO_PassNum_4 = KeyValue_RDD.filter(lambda x: x[1][0] == '4'                                                   ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)
Top_PUDO_PassNum_5 = KeyValue_RDD.filter(lambda x: x[1][0] == '5'                                                   ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)
Top_PUDO_PassNum_6 = KeyValue_RDD.filter(lambda x: x[1][0] == '6'                                                   ).sortByKey(ascending = False).map(lambda x: (x[1],x[0])).take(5)

# Print Results
'''
print('@@@@@@@', 'Num=1', Top_PUDO_PassNum_1, '\n')
print('@@@@@@@', 'Num=2', Top_PUDO_PassNum_2, '\n')
print('@@@@@@@', 'Num=3', Top_PUDO_PassNum_3, '\n')
print('@@@@@@@', 'Num=4', Top_PUDO_PassNum_4, '\n')
print('@@@@@@@', 'Num=5', Top_PUDO_PassNum_5, '\n')
print('@@@@@@@', 'Num=6', Top_PUDO_PassNum_6, '\n')
'''


# Create DataFrame

df = pd.DataFrame({})
df['1 Passenger'] = Top_PUDO_PassNum_1
df['2 Passengers'] = Top_PUDO_PassNum_2
df['3 Passengers'] = Top_PUDO_PassNum_3
df['4 Passengers'] = Top_PUDO_PassNum_4
df['5 Passengers'] = Top_PUDO_PassNum_5
df['6 Passengers'] = Top_PUDO_PassNum_6


print('#######', 'DataFrame\n', df)




