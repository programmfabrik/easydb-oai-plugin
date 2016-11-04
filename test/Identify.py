#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH Identify')
argparser.add_argument('url', help='OAI/PMH URL')
args = argparser.parse_args()

sickle = Sickle(args.url)
identify_information = sickle.Identify()

parameters = [
    'repositoryName',
    'baseURL',
    'earliestDatestamp',
    'deletedRecord',
    'granularity'
]

table = [[p, getattr(identify_information, p)] for p in parameters]
print (tabulate(table, headers=['parameter', 'value']))
