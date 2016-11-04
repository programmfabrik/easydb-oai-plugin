#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH ListIdentifiers')
argparser.add_argument('url', help='OAI/PMH URL')
argparser.add_argument('metadataPrefix', help='metadataPrefix')
argparser.add_argument('--set', help='set')
args = argparser.parse_args()

sickle = Sickle(args.url)
identifiers = sickle.ListIdentifiers(metadataPrefix=args.metadataPrefix, set=args.set)

parameters = [
    'identifier',
    'datestamp',
    'setSpecs'
]

table = [[getattr(i, p) for p in parameters] for i in identifiers]
print (tabulate(table, headers=parameters))
