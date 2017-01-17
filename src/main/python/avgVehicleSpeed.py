#!/usr/bin/env python
import sys
import os
import json
import pyspark

def getVehicleSpeed(row):
    try:
        speed = [float(row[columnToIndex['avgVehicleSpeed']])]
    except:
        speed = []
    return speed

# add actual job
def doJob(rdd):
    print('avg speed job')
    #Map column names to column indices
    columns = ['measurementSiteReference','measurementSiteVersion','index','periodStart','periodEnd','numberOfIncompleteInputs','numberOfInputValuesused','minutesUsed','computationalMethod','standardDeviation','supplierCalculatedDataQuality','sCDQ_Low','sCDQ_SD','number_of_sCDQ','dataError','travelTimeType','avgVehicleFlow','avgVehicleSpeed','avgTravelTime','computationMethod','measurementEquipmentTypeUsed','measurementSiteName1','measurementSiteName2','measurementSiteNumberOfLanes', 'measurementSiteIdentification','measurementSide','accuracy','period','specificLane','specificVehicleCharacteristics','startLocatieForDisplayLat','startLocatieForDisplayLong','LocationCountryCode','LocationTableNumber','LocationTableVersion','alertCDirectionCoded','specificLocation','offsetDistance','LOC_TYPE','LOC_DES','ROADNUMBER','ROADNAME,FIRST_NAME,SECND_NAME','messageType','publicationTime','deducedNoTrafficMinutes','carriageway']

    columnToIndex = {}
    for index, column in enumerate(columns):
      columnToIndex[column] = index
      #print(columnToIndex)
    fifteen = rdd.take(15)
    for row in fifteen:
        print(row)
        print(row[columnToIndex['avgVehicleSpeed']])
    print(fifteen[2])
    print(fifteen[2][columnToIndex['avgVehicleSpeed']])
    total = rdd.count()
    avgSpeed = rdd.flatMap(getVehicleSpeed).reduce(lambda v1, v2: v1 + v2) / total
    #print(rdd.first())
    print('Average vehicle speed', avgSpeed)

    return rdd

def main():
  # parse arguments
  in_dir, out_dir = sys.argv[1:]

  conf = pyspark.SparkConf().setAppName("%s %s %s" % (os.path.basename(__file__), in_dir, out_dir))
  sc = pyspark.SparkContext(conf=conf)

  # invoke job and put into output directory
  doJob(sc.textFile(in_dir)).saveAsTextFile(out_dir)

if __name__ == '__main__':
  main()
