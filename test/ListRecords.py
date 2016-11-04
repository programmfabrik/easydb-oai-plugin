#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH ListRecords')
argparser.add_argument('url', help='OAI/PMH URL')
argparser.add_argument('metadataPrefix', help='metadataPrefix')
argparser.add_argument('--set', help='set')
args = argparser.parse_args()

sickle = Sickle(args.url)
records = sickle.ListRecords(metadataPrefix=args.metadataPrefix, set=args.set)

parameters = [
    'identifier',
    'datestamp',
    'setSpecs'
]

for record in records:
    table = [[p, getattr(record.header, p)] for p in parameters]
    print (tabulate(table, headers=parameters) + '\n')
