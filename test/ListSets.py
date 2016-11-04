#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH ListSets')
argparser.add_argument('url', help='OAI/PMH URL')
args = argparser.parse_args()

sickle = Sickle(args.url)
sets = sickle.ListSets()

parameters = [
    'setName',
    'setSpec'
]

table = [[getattr(s, p) for p in parameters] for s in sets]
print (tabulate(table, headers=parameters))
