#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH ListMetadataFormats')
argparser.add_argument('url', help='OAI/PMH URL')
args = argparser.parse_args()

sickle = Sickle(args.url)
formats = sickle.ListMetadataFormats()

parameters = [
    'metadataPrefix',
    'metadataNamespace',
    'schema'
]

table = [[getattr(mdf, p) for p in parameters] for mdf in formats]
print (tabulate(table, headers=parameters))
