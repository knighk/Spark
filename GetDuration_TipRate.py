'''Ideas

1.) Tip amount as % of cost by number of customers
2.) Piquete travel times.
'''

from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster('local[*]').setAppName('CcirelliCode')
sc = SparkContext(conf = conf)

from dateutil import parser
import pandas as pd
import os

# Import CSV File
Data = sc.textFile(r'yellow_tripdata_2017-02.csv')

# Original Data Set: Column Names / Position
'''
[u'VendorID', u'tpep_pickup_datetime', u'tpep_dropoff_datetime', u'passenger_count', u'trip_distance', u'RatecodeID', u'store_and_fwd_flag', u'PULocationID', u'DOLocationID', u'payment_type', u'fare_amount', u'extra', u'mta_tax', u'tip_amount', u'tolls_amount', u'improvement_surcharge', u'total_amount']
'''

# Create Table
Data_splitNewLine = Data.map(lambda x: x.split(','))\
                        .filter(lambda x: len(x) > 1)\
                        .filter(lambda x: 'VendorID' not in x)


# Isolate Hour / Minute for PU/DO Times 
'''Documentation
- Pick-up time:         x[1][10:16]
- Drop-off time:        x[2][10:16]  
- Operation:            DOtime - PUtime


Trip_Duration_minutes_RDD = Data_splitNewLine.map(
                lambda x:((parser.parse(x[2][10:16]) - parser.parse(x[1][10:16]))\
                .total_seconds() / 60))\
                .filter(lambda x: x>0)    # exclude zero values
'''

# Get Median, Min, and Max:     
'''Documentation:
- SampeSize     = 10,000
- Max           = 1,429
- Median        = 9
- Min           = 1

Sample_RDD = Trip_Duration_minutes_RDD.take(10000)
df = pd.DataFrame(Sample_RDD)
print('@@@@@@@@@', '\n', 'Medain =>',df.median(), '\n', 'Max =>', df.max(), '\n', 
        'Min =>', df.min())
'''


# Get Median Upper Bound
'''Documentation:
- Filter:               Limit RDD to upper bound
- Median_upper:         16 was the median for the upper bound. 

UpperHalf_RDD = Trip_Duration_minutes_RDD.filter(lambda x: x > 9).take(10000)
df_median = pd.DataFrame(UpperHalf_RDD).median()
print('@@@@@@@', 'Median =>', df_median)
'''

# Get Median Lower Bound
'''Documentation:
- Filter:               Limit RDD to lower bound (== or less than 9)
- Median_lower:         5 

LowerHalf_RDD = Trip_Duration_minutes_RDD.filter(lambda x: x < 9 or x == 9).take(10000)
df_median = pd.DataFrame(LowerHalf_RDD).median()
print('@@@@@@', 'Median =>', df_median)
'''


# DURATION QUARTILES:
'''Documentation
- Q1 =  0  to 5
- Q2 =  6  to 9
- Q3 =  10 to 16
- Q4 =     >  16
'''


# FINAL FUNCTION - TRANSFORM RDD 

def tranform_RDD(RDD):
        VendorID =              RDD[0]
        PU_datetime =           RDD[1]
        DO_datetime=            RDD[2]
        passenger_count =       RDD[3]
        trip_distance =         RDD[4]
        RatecodeID =            RDD[5]
        store_and_fwd_flag=     RDD[6]
        PULocationID=           RDD[7]
        DOLocationID=           RDD[8]
        payment_type=           RDD[9]
        fare_amount=            RDD[10]
        extra=                  RDD[11]
        mta_tax=                RDD[12]
        tip_amount=             float(RDD[13])
        tolls_amount=           RDD[14]
        improvement_surcharge=  RDD[15]
        total_amount=           float(RDD[16])
        # Calculate Trip Duration
        Trip_duration_minutes = (((parser.parse(DO_datetime[10:16])\
                                 - parser.parse(PU_datetime[10:16]))\
                                .total_seconds())/ 60)
        # Calculate Tip Rate
        Tip_rate =              round((tip_amount / total_amount),2)
        # Categorize Trip Duration into Quartiles i-iv
        Duration_quartile = ''
        if Trip_duration_minutes < 5 or Trip_duration_minutes == 5:
                Duration_quartile = '1Q'
        elif Trip_duration_minutes > 5 and Trip_duration_minutes < 9\
                                        or Trip_duration_minutes == 9:
                Duration_quartile = '2Q'
        elif Trip_duration_minutes > 9 and Trip_duration_minutes < 16\
                                        or Trip_duration_minutes == 16:
                Duration_quartile = '3Q'
        elif Trip_duration_minutes > 16:
                Duration_quartile = '4Q'

        # Return Original Data Set with the Addition of TripDuration, TipRate, DurationQ
        return  VendorID, PU_datetime, DO_datetime, passenger_count, trip_distance,\
                RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type,\
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,\
                tip_amount, Trip_duration_minutes, Duration_quartile, Tip_rate

# Map Transformation Function to RDD
RDD_add_Duration_TipRate = Data_splitNewLine.map(tranform_RDD)

# Print Results
print('#######', RDD_add_Duration_TipRate.take(10))

# Write Results to Text File
RDD_add_Duration_TipRate.saveAsTextFile('/home/ccirelli1/Scripts/RDD_Transformed.txt')























