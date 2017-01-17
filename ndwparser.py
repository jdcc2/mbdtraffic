#http://www.diveintopython3.net/xml.html

import click
import requests
import io
import os
#import xml.etree.ElementTree as etree
from lxml import etree
import time
import gzip
#For HTTP last modified header parsing
import email.utils as eut

@click.group()
def cli():
    pass

#Take a etree xml root and write valid ndw data to a csv row in file
def trafficSpeedXMLToCSV(root, outfile):
    success = 0
    errors = 0
    #Search for siteMeasurement under:
    #soap:envelope -> soap:body -> exchange -> payloadPublication
    for child in root[0][0][1].findall('{http://datex2.eu/schema/2/2_0}siteMeasurements'):
        #Extract the measurementSiteReference.id and measurementTimeDefault content
        msmSite = child.find('{*}measurementSiteReference').attrib['id']
        msmTime = child.find('{*}measurementTimeDefault').text
        msvs = child.findall('{*}measuredValue')
        for msv in msvs:
            msv2 = msv.find('{*}measuredValue')
            bv = msv2.find('{*}basicData')
            type = bv.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
            value = ''
            error = False
            if type == 'TrafficSpeed':
                value = bv.find('{*}averageVehicleSpeed').find('{*}speed').text
            elif type == 'TrafficFlow':
                dataError = bv.find('{*}vehicleFlow').find('{*}dataError')
                error = not dataError is None and dataError.text == 'true'
                if not error:
                    value = bv.find('{*}vehicleFlow').find('{*}vehicleFlowRate').text
            if not error:
                outfile.write(','.join([msmSite, msmTime, type, value]) + '\n')
                success += 1
            else:
                errors += 1
    print('Number of lines produced: {}'.format(success))
    print('Number of measurements with omitted due to error: {}'.format(errors))


@click.command()
@click.option('--interval', default=60, help='fetch interval')
@click.option('--outputdir', default='./', help='directory to write the csv files to')
def fetch(interval, outputdir):
    print('Fetching traffic data every {} seconds'.format(interval))
    lastModified = None

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
                date = list(eut.parsedate(lastModified))
                outputfile = os.path.join(outputdir, 'ndw_trafficspeed_{}_{}_{}_{}_{}_{}.csv'.format(*date[:6]))
                with open(outputfile, 'w') as out:
                    trafficSpeedXMLToCSV(etree.fromstring(unzipped), out)
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
    cli()
