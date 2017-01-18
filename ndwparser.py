#http://www.diveintopython3.net/xml.html

import click
import requests
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

@click.group()
def cli():
    pass


def trafficSpeedXMLToCSV(root, outfile, siteData=None):
    """
    Take an etree xml root and write valid ndw data to a csv row in file

    #Output CSV file row format(between braces is only in the output when siteData is available):
    measurementSiteId, measurementIndex, measurementTime, measurementType, value, measurementSiteName, nrOfLanes, period

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

def measurementSiteXMLToDict(root):
    """
    Parse the measurement site XML from the NDW open data and return a dict structured as follows:
    { <siteID> :    {
                        {
                            'measurementSiteId': siteID,
                            'measurementSiteName': msmSiteName,
                            'nrOfLanes': nrOfLanes,
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
    for msmSiteRecord in msmSiteTable.findall('{http://datex2.eu/schema/2/2_0}measurementSiteRecord'):
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
    return output

@click.command()
@click.argument('xmlinput')
def msmconvert(xmlinput):
    tree = etree.parse(xmlinput)
    root = tree.getroot()
    #with open(csvoutput, 'w') as outfile:
    print(measurementSiteXMLToDict(root))


@click.command()
@click.option('--interval', default=60, help='fetch interval')
@click.option('--outputdir', default='./', help='directory to write the csv files to')
@click.option('--sitexml', default=None, help='path to XML file containing additional measurement site data to enrich measurement data')
@click.option('--hdfs', default=False, is_flag=True, help='write the file hdfs in outputdir')
def fetch(interval, outputdir, sitexml, hdfs):
    print('Fetching traffic data every {} seconds'.format(interval))
    lastModified = None
    siteData = None
    if sitexml:
        siteDataRoot = etree.parse(sitexml).getroot()
        siteData = measurementSiteXMLToDict(siteDataRoot)
        print('Parsed site data XML')
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
    cli.add_command(msmconvert)
    cli.add_command(fetch)
    cli()
