import numpy as np
from pyspark import SparkContext, SparkConf
from dateutil import parser
import pandas as pd
import os

conf = SparkConf().setMaster('local[*]').setAppName('CcirelliCode')
sc = SparkContext(conf = conf)

path="/home/owen/Documents/untitled/project/data/"
# Data = sc.textFile(path+r'yellow_tripdata_2017-02.csv')
Data=sc.textFile("file:/home/owen/Documents/untitled/project/data/*.csv")

Data_splitNewLine = Data.map(lambda x: x.split(','))\
                        .filter(lambda x: len(x) > 1)\
                        .filter(lambda x: 'VendorID' not in x)\
                        .filter(lambda x: float(x[16])!=0)

def tranform_RDD(RDD, tag):
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
        if tag=="time":
            return  VendorID, PU_datetime, DO_datetime, passenger_count, trip_distance,\
                RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type,\
                fare_amount, extra, mta_tax, tip_amount, tolls_amount,\
                tip_amount, Trip_duration_minutes, Duration_quartile, Tip_rate
        elif tag=="company":
            return VendorID, Tip_rate
        elif tag=="passengers":
            return passenger_count, Tip_rate
        elif tag=="route":
            return (PULocationID,DOLocationID), Tip_rate


def describe(listData):
    percentile_25 = np.percentile(np.array(listData), 25)
    percentile_50 = np.percentile(np.array(listData), 50)
    percentile_75 = np.percentile(np.array(listData), 75)
    minValue = min(listData)
    maxValue = max(listData)
    return percentile_25,percentile_50,percentile_75,minValue,maxValue

# Map Transformation Function to RDD
# RDD_add_Duration_TipRate = Data_splitNewLine.map(lambda x: tranform_RDD(x,"time")).persist()
RDD_add_Company_TipRate = Data_splitNewLine.map(lambda x: tranform_RDD(x,"company"))
RDD_add_Passengers_TipRate = Data_splitNewLine.map(lambda x: tranform_RDD(x,"passengers"))
# RDD_add_route_TipRate = Data_splitNewLine.map(lambda x: tranform_RDD(x,"route")).persist()

#process tips based on company
RDD_reduced_company=RDD_add_Company_TipRate.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
count=RDD_add_Company_TipRate.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y).collect()
count1,count2=[count[0][1],count[1][1]]

RDD_sum_tips_company=RDD_add_Company_TipRate.reduceByKey(lambda x,y:x+y).collect()
avg1=RDD_sum_tips_company[0][1]/count1
avg2=RDD_sum_tips_company[1][1]/count2
RDD_tips_company_1=RDD_add_Company_TipRate.filter(lambda x: x[0]=="1").map(lambda x:x[1])
RDD_tips_company_2=RDD_add_Company_TipRate.filter(lambda x: x[0]=="2").map(lambda x:x[1])
lookat=RDD_tips_company_2.filter(lambda x:float(x)<0)
list_tips_company_1=RDD_tips_company_1.collect()
describe_company_1=describe(list_tips_company_1)
list_tips_company_2=RDD_tips_company_2.collect()
describe_company_2=describe(list_tips_company_2)

# df=pd.DataFrame(list(list_tips_company_1))
# df.to_csv("list_tips_company_1.csv")

#process tips based on passenger number
RDD_reduced_passenger=RDD_add_Passengers_TipRate.reduceByKey(lambda x,y: (x+y))
RDD_count_passenger=RDD_add_Passengers_TipRate.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)
list_count_passenger=RDD_count_passenger.collect()
#[('4', 1), ('6', 1), ('1', 1), ('5', 1), ('3', 1), ('2', 1)]
# [('8', 79), ('9', 65), ('0', 1710), ('5', 1439537), ('6', 856277), ('2', 4189702), ('7', 72), ('3', 1173063), ('1', 20967922), ('4', 541991)]

list_count_passenger_number=[i[0] for i in list_count_passenger]
list_count_passenger_count=[i[1] for i in list_count_passenger]

list_sum_tips_passenger=RDD_add_Passengers_TipRate.reduceByKey(lambda x,y:x+y).collect()
list_avg_tips_passenger=np.array([i[1] for i in list_sum_tips_passenger])/np.array(list_count_passenger_count)
list_add_Passengers_TipRate=RDD_add_Passengers_TipRate.filter(lambda x: x[0] == "0").map(lambda x: x[1]).collect()
describe_passengers_1=describe(list_add_Passengers_TipRate)

# Print Results
# print('#######', RDD_add_Duration_TipRate.take(10))

# Write Results to Text File
# RDD_add_Duration_TipRate.saveAsTextFile('RDD_add_Duration_TipRate.txt')

# data_Company_TipRate=RDD_add_Company_TipRate.collect()
# df=pd.DataFrame(list(data_Company_TipRate))
# df.to_csv("companyTipRate.csv")

# data_Passengers_TipRate=RDD_add_Passengers_TipRate.collect()
# df=pd.DataFrame(list(data_Passengers_TipRate))
# df.to_csv("passengersTipRate.csv")