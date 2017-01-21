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
    #rows with an average vehicle speed <= 60 and > 0 (class anyVehicle is all vehicles)
    split = rdd.map(lambda line: line.split(','))
    vsLow = split.filter(lambda row: row[columns['measurementType']] == 'TrafficSpeed' and row[columns['vehicleClass']] == 'anyVehicle' and float(row[columns['value']]) <= 60 and float(row[columns['value']]) > 30)
    #all rows with flow measurements for all vehicle types
    flows = split.filter(lambda row: row[columns['measurementType']] == 'TrafficFlow' and row[columns['vehicleClass']] == 'anyVehicle')

    #convert both rdds to key/value pairs with the measurement location id and time as the keys and the rows as the value
    vslowkv = vsLow.map(lambda row: (row[columns['measurementSiteId']] + row[columns['measurementTime']], row))
    flowskv = flows.map(lambda row: (row[columns['measurementSiteId']] + row[columns['measurementTime']], row))

    merged = vslowkv.join(flowskv)

    #get the rows were the vehiclespeed is low and the flow > 10
    #(K, (speed, flow))
    #check that the second part of the value tuple is actually a flow measurement
    jams = merged.filter(lambda pair: pair[1][1][columns['measurementType']] == 'TrafficFlow' and int(pair[1][1][columns['value']]) >= 10 )

    #TODO convert the remaining rows in a suitable data point for display
    #NOTE the value in TrafficFlow flow is amount of vehicles that would have passed in an hour if the flow continued like the currently measured period
    #so look at he value and the period to calculate the actual in the measurement
    #NOTE maybe exclude data points that have a period greater than 60 seconds, as data is retrieved every 60 seconds and duplicates may occur
    #(not sure about this, maybe these longer period measurements do not show up every minutes in the measurements file)

    print('done')
    print(jams.count())
    for item in jams.take(15):
        print(item[0], item[1][0], item[1][1])
    return jams

def main():
    # parse arguments
    in_dir, out_dir = sys.argv[1:]

    conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
    sc = pyspark.SparkContext(conf=conf)

    # invoke job and put into output directory
    doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
    main()