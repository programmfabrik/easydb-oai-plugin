# -*- coding: utf-8 -*-

import oai_modules.request
import oai_modules.response

class Repository(object):
    def __init__(self, easydb_context, base_url, name, admin_email):
        self.easydb_context = easydb_context
        self.base_url = base_url
        self.name = name
        self.admin_email = admin_email
    def process_request(self, parameters):
        try:
            request = oai_modules.request.Request.parse(self, parameters)
            return request.process()
        except oai_modules.request.ParseError as e:
            request = oai_modules.request.Request(self)
            return oai_modules.response.Error(request, e.error_code, e.error_message)
    def get_sets(self):
        # FIXME: sort by hierarchy for flow control
        sets = [Set('Pools', 'pool'), Set('Collections', 'collection')]
        query = {
            'type': 'pool',
            'generate_rights': False
        }
        response = self.easydb_context.search('user', 'deep_link', query)
        for pool in response['objects']:
            spec = ':'.join(['pool'] + list(map(lambda element: str(element['pool']['_id']), pool['_path'])))
            # FIXME: language
            name = pool['_path'][-1]['pool']['name']['de-DE']
            sets.append(Set(name, spec))
        query = {
            'type': 'collection',
            'generate_rights': False
        }
        response = self.easydb_context.search('user', 'deep_link', query)
        for collection in response['objects']:
            spec = ':'.join(['collection'] + list(map(lambda element: str(element['collection']['_id']), collection['_path'])))
            # FIXME: language
            name = collection['_path'][-1]['collection']['displayname']['de-DE']
            sets.append(Set(name, spec))
        return sets

class Set(object):
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec
