#!/usr/bin/env python
import sys
import os
import json
import pyspark

# add actual job
def doJob(rdd):
    """
    Select all vehicleSpeed rows were the average vehicle speed dropped below 30Kph/
    Then merge all vehicleFlow rows for the vehicleSpeed rows found based on time and measurement location.
    Return all merged rows were the average vehicle speed is below 30 and the flow above 10 vehicles per minute

    :param rdd:
    :return:
    """
    columns = {
        "measurementSiteId": 0,
        "measurementIndex": 1,
        "measurementTime": 2,
        "measurementType": 3,
        "value": 4,
        "measurementSiteName": 5,
        "latitude": 6,
        "longitude": 7,
        "nrOfLanes": 8,
        "specificLane": 9,
        "period": 10,
        "vehicleClass": 11
    }
    print('start')
    #rows with a trafficspeed value of -1 shoudl be excluded
    #rows with an average vehicle speed <= 35 and > 0 (class anyVehicle is all vehicles)
    split = rdd.map(lambda line: line.split(','))
    vsLow = split.filter(lambda row: row[columns['measurementType']] == 'TrafficSpeed' and row[columns['vehicleClass']] == 'anyVehicle' and float(row[columns['value']]) <= 35 and float(row[columns['value']]) > 30)
    #all rows with flow measurements for all vehicle types
    flows = split.filter(lambda row: row[columns['measurementType']] == 'TrafficFlow' and row[columns['vehicleClass']] == 'anyVehicle')

    #convert both rdds to key/value pairs with the measurement location id, time, lane as the keys and the rows as the value
    vslowkv = vsLow.map(lambda row: (row[columns['measurementSiteId']] + row[columns['measurementTime']] + row[columns['specificLane']], row))
    flowskv = flows.map(lambda row: (row[columns['measurementSiteId']] + row[columns['measurementTime']] + row[columns['specificLane']], row))

    merged = vslowkv.join(flowskv)

    #get the rows were the vehiclespeed is low and the flow > 10
    #(K, (speedrow, flowrow)) -> K = siteId + timeID combination
    #check that the second part of the value tuple is actually a flow measurement
    #flow measurment is vehicle per hour, convert to vehicles per minute, using the measurement period
    jams = merged.filter(lambda pair: pair[1][1][columns['measurementType']] == 'TrafficFlow' and int(pair[1][1][columns['value']]) / (3600 / float(pair[1][1][columns['period']])) >= 10)

    #TODO convert the remaining rows in a suitable data point for display
    #NOTE the value in TrafficFlow flow is amount of vehicles that would have passed in an hour if the flow continued like the currently measured period
    #so look at he value and the period to calculate the actual in the measurement
    #NOTE maybe exclude data points that have a period greater than 60 seconds, as data is retrieved every 60 seconds and duplicates may occur
    #(not sure about this, maybe these longer period measurements do not show up every minutes in the measurements file)
    #print('Jams: ', jams.count())

    #output dict -> {'siteid', 'time', 'sitename', 'latitude', 'longitude', 'lane', 'nroflanes', 'avgspeed', 'flowpermin', 'period', }
    #outputs a tuple with the measurment time as the key, and a dict with all info as the value
    formattedJams = jams.map(lambda pair: (pair[1][0][columns['measurementTime']],
                                            {'measurementSiteId': pair[1][0][columns['measurementSiteId']],
                                             'measurementTime': pair[1][0][columns['measurementTime']],
                                             'measurementSiteName': pair[1][0][columns['measurementSiteName']],
                                             'latitude': float(pair[1][0][columns['latitude']]),
                                             'longitude': float(pair[1][0][columns['longitude']]),
                                             'lane': pair[1][0][columns['specificLane']],
                                             'nrOfLanes': pair[1][0][columns['nrOfLanes']],
                                             'averageSpeed': float(pair[1][0][columns['value']]),
                                             'averageFlow': int(pair[1][1][columns['value']]) / (3600 / float(pair[1][1][columns['period']])),
                                             'period': float(pair[1][1][columns['period']])}))\
        .groupByKey()\
        .sortByKey(True) #sort in ascending order
    print('done3')
    print(formattedJams.count())
    first = formattedJams.first()
    print(first[0])
    for item in first[1]:
        print(item)

    # for item in formattedJams.take(15):
    #     print(item)
    return formattedJams

def main():
    # parse arguments
    in_dir, out_dir = sys.argv[1:]

    conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
    sc = pyspark.SparkContext(conf=conf)

    # invoke job and put into output directory
    doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
    main()