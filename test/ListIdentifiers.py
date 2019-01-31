#!/usr/bin/python

from argparse import ArgumentParser
from sickle import Sickle
from tabulate import tabulate

argparser = ArgumentParser(description='OAI/PMH ListIdentifiers')
argparser.add_argument('url', help='OAI/PMH URL')
argparser.add_argument('metadataPrefix', help='metadataPrefix')
argparser.add_argument('--set', help='set', dest='set_filter')
argparser.add_argument('--from', help='from', dest='from_filter')
argparser.add_argument('--until', help='until', dest='until_filter')
args = argparser.parse_args()

sickle = Sickle(args.url)
parameters = {
    'metadataPrefix': args.metadataPrefix,
    'set': args.set_filter,
    'from': args.from_filter,
    'until': args.until_filter
}
identifiers = sickle.ListIdentifiers(**parameters)

parameters = [
    'identifier',
    'datestamp',
    'setSpecs'
]


def attr_to_str(attr):
    if isinstance(attr, list):
        return ', '.join(attr)
    return attr


table = [[attr_to_str(getattr(i, p)) for p in parameters] for i in identifiers]
print (tabulate(table, headers=parameters))
