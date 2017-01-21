#http://www.diveintopython3.net/xml.html

#Standard lib import
import io
import os
#lxml's etree implemenation is faster but not always available
try:
    from lxml import etree
except:
    import xml.etree.ElementTree as etree
import time
import gzip
import datetime
#For HTTP last modified header parsing
import email.utils as eut
from subprocess import Popen, PIPE
#External imports
import overpy
import click
import requests
import json
from collections import OrderedDict

@click.group()
def cli():
    pass


@click.command()
def testgetspeed():
    getMaxSpeedRoad(52.0265, 4.68309)

def getMaxSpeedRoad(latitude, longitude, radius=5):
    """
    Use the OpenStreetMaps Overpass QL API to get the maximum speed of the road at the given coordinates

    Link: http://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL
    Link: https://github.com/DinoTools/python-overpy

    :param latitude: WD84 latitude
    :param longitude: WD84 longitude
    :return:
    """
    api = overpy.Overpass()
    radius = radius
    #Only search for primary and secondary roads with a maxspeed tag
    query = "way (around:{},{},{}) [\"highway\"=\"primary\"] [\"highway\"=\"secondary\"] [\"maxspeed\"]; out body;".format(radius, latitude, longitude)
    # fetch all ways and nodes
    response = api.query(query)
    #match a road for the coordinates, 2 roads can be two directions of one road, more than 3 is undecidable
    length = len(response.ways)
    result = None
    if length == 1:
        result = response.ways[0].tags.get("maxspeed")
    elif length == 2: #possibly two directions of the same road, check if the maxspeed is equal, that's the only thing that matters
        #response.ways[0].tags.get("name", "1") == response.ways[1].tags.get("name", "2") -> not necessary
        if response.ways[0].tags.get("maxspeed") == response.ways[1].tags.get("maxspeed") and response.ways[0].tags.get("maxspeed") != "":
            result = response.ways[0].tags.get("maxspeed")
    return result

def trafficSpeedXMLToCSV(root, outfile, siteData=None):
    """
    Take an etree xml root and write valid ndw data to a csv row in file

    #Output CSV file row format(between braces is only in the output when siteData is available):
    measurementSiteId, measurementIndex, measurementTime, measurementType, value, measurementSiteName, latitude, longitude, maxspeed, nrOfLanes, specificLane, period, vehicleClass

    #measurementSpecificCharacteristics
    From the docs:
    Het element measurementSpecificCharacteristics komt per meetlocatie een of meer
    keren voor en beschrijft steeds een meetpunt, gegevenstype, voertuigcategorie trio

    #Combining measurement site data:
    specificMeasurementCharacteristic (measurement point info) and measurementValue (a measured value)
    both contain an index property which is used to match them together

    :param root: etree XML root of a NDW trafficspeed measurement XML file
    :param outfile: path to write the output CSV to
    :param siteData: path to XML file containing measurement site data
    :return:
    """
    success = 0
    errors = 0
    #Search for siteMeasurement under:
    #soap:envelope -> soap:body -> exchange -> payloadPublication
    for child in root[0][0][1].findall('{http://datex2.eu/schema/2/2_0}siteMeasurements'):
        #Extract the measurementSiteReference.id and measurementTimeDefault content
        msmSite = child.find('{http://datex2.eu/schema/2/2_0}measurementSiteReference').attrib['id']
        msmTime = child.find('{http://datex2.eu/schema/2/2_0}measurementTimeDefault').text
        msvs = child.findall('{http://datex2.eu/schema/2/2_0}measuredValue')
        measurementSiteData = None
        if siteData is not None and msmSite in siteData:
            measurementSiteData = siteData[msmSite]
        else:
            print('measurementSiteData is None')
            print(siteData)
            print(msmSite)


        for msv in msvs:
            index = msv.attrib['index']
            msv2 = msv.find('{http://datex2.eu/schema/2/2_0}measuredValue')
            bv = msv2.find('{http://datex2.eu/schema/2/2_0}basicData')
            msmType = bv.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
            value = ''
            error = False
            if msmType == 'TrafficSpeed':
                value = bv.find('{http://datex2.eu/schema/2/2_0}averageVehicleSpeed').find('{http://datex2.eu/schema/2/2_0}speed').text
            elif msmType == 'TrafficFlow':
                dataError = bv.find('{http://datex2.eu/schema/2/2_0}vehicleFlow').find('{http://datex2.eu/schema/2/2_0}dataError')
                error = not dataError is None and dataError.text == 'true'
                if not error:
                    value = bv.find('{http://datex2.eu/schema/2/2_0}vehicleFlow').find('{http://datex2.eu/schema/2/2_0}vehicleFlowRate').text
            if not error:
                measurementCharacteristic = None
                #Combine additional measurement site data if availale
                if measurementSiteData is not None and index in measurementSiteData['measurementSpecificCharacteristics']:
                    measurementCharacteristic = measurementSiteData['measurementSpecificCharacteristics'][index]

                csvrow = [msmSite, index, msmTime, msmType, value]

                if measurementCharacteristic is not None:
                    csvrow.append(measurementSiteData['measurementSiteName'])
                    csvrow.append(measurementSiteData['latitude'])
                    csvrow.append(measurementSiteData['longitude'])
                    csvrow.append(measurementSiteData['maxspeed'])
                    csvrow.append(measurementSiteData['nrOfLanes'])
                    csvrow.append(measurementCharacteristic['specificLane'])
                    csvrow.append(measurementCharacteristic['period'])
                    csvrow.append(measurementCharacteristic['vehicleClass'])

                #None values are converted by str() call
                outfile.write(','.join(str(value) for value in csvrow) + '\n')
                success += 1
            else:
                errors += 1
    print('Number of lines produced: {}'.format(success))
    print('Number of measurements with omitted due to error: {}'.format(errors))


def measurementSiteXMLToDict(root, enablemaxspeed=False):
    #NOTE: Usually querying the Overpass API for the max speed of  all measurement locations times out
    """
    Parse the measurement site XML from the NDW open data and return a dict structured as follows:
    { <siteID> :    {
                        {
                            'measurementSiteId': siteID,
                            'measurementSiteName': msmSiteName,
                            'nrOfLanes': nrOfLanes,
                            'latitude': latitude,
                            'longitude': longitude,
                            'maxspeed': 'maxspeed',
                            'measurementSpecificCharacteristics':
                                {
                                    <index> :
                                        {
                                            'index': index,
                                            'period': period,
                                            'specificLane': specificLane,
                                            'specificMeasurementValueType': specificMeasurementValueType
                                        }
                                }
                        }
                    }

    }

    """
    #print(root[0][0][1].tag)
    #print(root[0][0][1].findall('{*}measurementSiteTable')[0].tag)
    #Extract the measurement site table (file assumed to contain only one) from the payload element
    output = {}
    msmSiteTable = root[0][0][1].find('{http://datex2.eu/schema/2/2_0}measurementSiteTable')
    records = msmSiteTable.findall('{http://datex2.eu/schema/2/2_0}measurementSiteRecord')
    total = len(records)
    current = 0
    haslocation = 0
    hasmaxspeed = 0
    print('%s / %s' % (current, total))
    for msmSiteRecord in msmSiteTable.findall('{http://datex2.eu/schema/2/2_0}measurementSiteRecord'):
        measurementSiteLocation = msmSiteRecord.find('{http://datex2.eu/schema/2/2_0}measurementSiteLocation')
        locationForDisplay = measurementSiteLocation.find('{http://datex2.eu/schema/2/2_0}locationForDisplay')
        current += 1
        print('%s / %s' % (current, total))
        latitude = None
        longitude = None
        maxspeed = None
        #check if the gps location is available and fetch the maximum speed using these coordinates
        if locationForDisplay is not None:
            latitude = locationForDisplay.find('{http://datex2.eu/schema/2/2_0}latitude').text
            longitude = locationForDisplay.find('{http://datex2.eu/schema/2/2_0}longitude').text
            if enablemaxspeed:
                maxspeed = getMaxSpeedRoad(latitude, longitude)
            haslocation += 1
        if maxspeed is not None:
            hasmaxspeed += 1
        siteID = msmSiteRecord.attrib['id']
        msmSiteName = msmSiteRecord.find('{http://datex2.eu/schema/2/2_0}measurementSiteName')\
            .find('{http://datex2.eu/schema/2/2_0}values')\
            .find('{http://datex2.eu/schema/2/2_0}value')\
            .text
        nrOfLanes = msmSiteRecord.find('{http://datex2.eu/schema/2/2_0}measurementSiteNumberOfLanes')
        #nrOfLanes is not always available
        if nrOfLanes is not None:
            nrOfLanes = nrOfLanes.text
        characteristics = msmSiteRecord.findall('{http://datex2.eu/schema/2/2_0}measurementSpecificCharacteristics')
        output[siteID] = {'measurementSiteId': siteID,
                          'measurementSiteName': msmSiteName,
                          'nrOfLanes': nrOfLanes,
                          'latitude': latitude,
                          'longitude': longitude,
                          'maxspeed': maxspeed,
                          'measurementSpecificCharacteristics': {}
                          }
        for msmChar in characteristics:
            index = msmChar.attrib['index']
            nestedMsmChar = msmChar.find('{http://datex2.eu/schema/2/2_0}measurementSpecificCharacteristics')
            period = nestedMsmChar.find('{http://datex2.eu/schema/2/2_0}period').text
            specificLane = nestedMsmChar.find('{http://datex2.eu/schema/2/2_0}specificLane')
            #specificLane is not always available
            if specificLane is not None:
                specificLane = specificLane.text
            #measurementCharacteristics contain either a vehicletype element or a lengthCharacteristic element describing the type of vehicle
            vehicleClass = ''
            specificVehicleCharacteristics = nestedMsmChar.find('{http://datex2.eu/schema/2/2_0}specificVehicleCharacteristics')
            vehicleType = specificVehicleCharacteristics.find('{http://datex2.eu/schema/2/2_0}vehicleType')
            lengthCharacteristic = specificVehicleCharacteristics.find('{http://datex2.eu/schema/2/2_0}lengthCharacteristic')
            if vehicleType is not None:
                vehicleClass = vehicleType.text
            elif lengthCharacteristic is not None:
                operator = lengthCharacteristic.find('{http://datex2.eu/schema/2/2_0}comparisonOperator').text
                lengthInMeters= lengthCharacteristic.find('{http://datex2.eu/schema/2/2_0}vehicleLength').text
                vehicleClass = operator + ' ' + lengthInMeters
            specificMeasurementValueType = nestedMsmChar.find('{http://datex2.eu/schema/2/2_0}specificMeasurementValueType').text
            output[siteID]['measurementSpecificCharacteristics'][index] = {'index': index,
                                                                           'period': period,
                                                                           'specificLane': specificLane,
                                                                           'specificMeasurementValueType': specificMeasurementValueType,
                                                                           'vehicleClass': vehicleClass
                                                                           }
    print('Total number of measurement sites: %s' % total)
    print('Number of sites with location: %s' % haslocation)
    print('Number of sites with maximum speed: %s' % hasmaxspeed)
    return output

@click.command()
def csvheader():
    """
    Note maxspeed header is not included

    :return:
    """
    columns = ['measurementSiteId', 'measurementIndex', 'measurementTime', 'measurementType', 'value', 'measurementSiteName', 'latitude', 'longitude', 'nrOfLanes', 'specificLane', 'period', 'vehicleClass']
    headersToColumns = OrderedDict()
    for index, item in enumerate(columns):
        headersToColumns[item] = index
    print(json.dumps(headersToColumns, indent=4))
    #print({item: index for index, item in enumerate(columns)})

@click.command(help='Convert NDW measurement site XML to JSON and try to add the maximum speed to the measurement locations')
@click.argument('xmlinput')
@click.argument('outputfile')
def msmsitejson(xmlinput, outputfile):
    tree = etree.parse(xmlinput)
    root = tree.getroot()
    # with open(csvoutput, 'w') as outfile:
    with open(outputfile, 'w') as out:
        json.dump(measurementSiteXMLToDict(root), out, indent=4)


@click.command()
@click.option('--interval', default=60, help='fetch interval')
@click.option('--outputdir', default='./', help='directory to write the csv files to')
@click.option('--sitexml', default=None, help='path to XML file containing additional measurement site data to enrich measurement data')
@click.option('--sitejson', default=None, help='path to JSON file containing additional measurement site data to enrich measurement data')
@click.option('--hdfs', default=False, is_flag=True, help='write the file hdfs in outputdir')
def fetch(interval, outputdir, sitexml, sitejson, hdfs):
    print('Fetching traffic data every {} seconds'.format(interval))
    lastModified = None
    siteData = None
    if sitejson:
        with open(sitejson, 'r') as sitejsonfile:
            siteData = json.loads(sitejsonfile.read())
    elif sitexml:
        siteDataRoot = etree.parse(sitexml).getroot()
        siteData = measurementSiteXMLToDict(siteDataRoot)
        print('Parsed measurement site data XML')
    else:
        print('No additional site data provided')
    while(True):
        r = requests.get('http://opendata.ndw.nu/trafficspeed.xml.gz')
        if(r.status_code) == 200:
            if r.headers['Last-Modified'] == lastModified:
                print('No new traffic data')
            else:
                lastModified = r.headers['Last-Modified']
                f = io.BytesIO(r.content)
                gzipped = gzip.GzipFile(filename=None, mode=None, compresslevel=9, fileobj=f, mtime=None)
                #unzipped = gzip.decompress(r.content)
                unzipped = gzipped.read()
                date = datetime.datetime(*list(eut.parsedate(lastModified))[:6])
                outputfile = os.path.join(outputdir, 'ndw_trafficspeed_{:%Y_%m_%d_%H_%M_%S}.csv'.format(date))
                if hdfs:
                    with io.BytesIO() as out:
                        p = Popen(["hdfs", "dfs", "-put", "-", outputfile], stdin=PIPE)
                        trafficSpeedXMLToCSV(etree.fromstring(unzipped), p.stdin, siteData=siteData)
                        p.stdin.close()
                        p.wait()
                        #p.flush()
                else:

                    with open(outputfile, 'w') as out:
                        trafficSpeedXMLToCSV(etree.fromstring(unzipped), out, siteData=siteData)

        else:
            print('Error status code')
        time.sleep(interval)

@click.command()
@click.argument('xmlinput')
@click.argument('csvoutput')
def convert(xmlinput, csvoutput):
    tree = etree.parse(xmlinput)
    root = tree.getroot()
    with open(csvoutput, 'w') as outfile:
        trafficSpeedXMLToCSV(root, outfile)

if __name__ == "__main__":
    cli.add_command(convert)
    cli.add_command(fetch)
    cli.add_command(testgetspeed)
    cli.add_command(msmsitejson)
    cli.add_command(csvheader)
    cli()
