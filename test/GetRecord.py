#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate
import xml.etree.ElementTree as ET
import xml.dom.minidom

argparser = ArgumentParser(description='OAI/PMH GetRecord')
argparser.add_argument('url', help='OAI/PMH URL')
argparser.add_argument('identifier', help='identifier')
argparser.add_argument('metadataPrefix', help='metadataPrefix')
args = argparser.parse_args()

sickle = Sickle(args.url)
record = sickle.GetRecord(identifier=args.identifier, metadataPrefix=args.metadataPrefix)

parameters = [
    'identifier',
    'datestamp',
    'setSpecs'
]

def attr_to_str(attr):
    if isinstance(attr, list):
        return ', '.join(attr)
    return attr

table = [[p, attr_to_str(getattr(record.header, p))] for p in parameters]
print ('\n=== Header:\n\n{}'.format(tabulate(table, headers=['variable', 'value'])))

print ('\n=== Record:\n\n{}'.format(xml.dom.minidom.parseString(ET.tostring(record.xml, 'UTF-8')).toprettyxml(indent='\t', encoding='UTF-8')))

