#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

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

table = [[p, getattr(record.header, p)] for p in parameters]
print (tabulate(table, headers=parameters))
