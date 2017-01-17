#http://www.diveintopython3.net/xml.html

import click
import xml.etree.ElementTree as etree
#from lxml import etree

#Take a etree xml root and write valid ndw data to a csv row in file
def trafficSpeedXMLToCSV(root, outfile):
    success = 0
    errors = 0
    #Search for siteMeasurement under:
    #soap:envelope -> soap:body -> exchange -> payloadPublication
    for child in root[0][0][1].findall('{http://datex2.eu/schema/2/2_0}siteMeasurements'):
        #Extract the measurementSiteReference.id and measurementTimeDefault content
        print(child.find('{http://datex2.eu/schema/2/2_0}measurementSiteReference'))
        msmSite = child.find('{http://datex2.eu/schema/2/2_0}measurementSiteReference').attrib['id']
        msmTime = child.find('{http://datex2.eu/schema/2/2_0}measurementTimeDefault').text
        msvs = child.findall('{http://datex2.eu/schema/2/2_0}measuredValue')
        for msv in msvs:
            msv2 = msv.find('{http://datex2.eu/schema/2/2_0}measuredValue')
            bv = msv2.find('{http://datex2.eu/schema/2/2_0}basicData')
            type = bv.attrib['{http://www.w3.org/2001/XMLSchema-instance}type']
            value = ''
            error = False
            if type == 'TrafficSpeed':
                value = bv.find('{http://datex2.eu/schema/2/2_0}averageVehicleSpeed').find('{http://datex2.eu/schema/2/2_0}speed').text
            elif type == 'TrafficFlow':
                dataError = bv.find('{http://datex2.eu/schema/2/2_0}vehicleFlow').find('{http://datex2.eu/schema/2/2_0}dataError')
                error = not dataError is None and dataError.text == 'true'
                if not error:
                    value = bv.find('{http://datex2.eu/schema/2/2_0}vehicleFlow').find('{http://datex2.eu/schema/2/2_0}vehicleFlowRate').text
            if not error:
                outfile.write(','.join([msmSite, msmTime, type, value]) + '\n')
                success += 1
            else:
                errors += 1
    print('Number of lines produced: {}'.format(success))
    print('Number of measurements with omitted due to error: {}'.format(errors))

@click.command()
@click.argument('xmlinput')
@click.argument('csvoutput')
def run(xmlinput, csvoutput):
    tree = etree.parse(xmlinput)
    root = tree.getroot()
    with open(csvoutput, 'w') as outfile:
        trafficSpeedXMLToCSV(root, outfile)

if __name__ == "__main__":
    run()
