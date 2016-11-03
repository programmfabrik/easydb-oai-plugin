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
        sets = []
        self.extend_sets(sets, 'pool')
        self.extend_sets(sets, 'collection')
        return sets
    def extend_sets(self, sets, base_type):
        sets.append(Set(set_names[base_type]['top'], base_type))
        query = {
            'type': base_type,
            'generate_rights': False,
            'sort': [
                {
                    'field': '{}._id'.format(base_type)
                }
            ]
        }
        response = self.easydb_context.search('user', 'oai_pmh', query)
        for obj in response['objects']:
            spec = ':'.join([base_type] + list(map(lambda element: str(element[base_type]['_id']), obj['_path'])))
            # FIXME: language
            name = obj['_path'][-1][base_type][set_names[base_type]['objkey']]['de-DE']
            sets.append(Set(name, spec))

set_names = {
    'pool': {
        'top': 'Pools',
        'objkey': 'name'
    },
    'collection': {
        'top': 'Collections',
        'objkey': 'displayname'
    }
}

class Set(object):
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec
