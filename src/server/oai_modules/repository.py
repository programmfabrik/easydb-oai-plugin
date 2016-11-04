# -*- coding: utf-8 -*-

from context import get_json_value
import oai_modules.request
import oai_modules.response

class Repository(object):
    def __init__(self, easydb_context, base_url, name, namespace_identifier, admin_email):
        self.easydb_context = easydb_context
        self.base_url = base_url
        self.name = name
        self.namespace_identifier = namespace_identifier
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
            spec = self.get_spec(obj['_path'], base_type)
            # FIXME: language
            name = obj['_path'][-1][base_type][set_names[base_type]['objkey']]['de-DE']
            sets.append(Set(name, spec))
    def get_spec(self, path_js, base_type):
        return ':'.join([base_type] + list(map(lambda element: str(element[base_type]['_id']), path_js)))
    def get_record(self, uuid):
        query = {
            'type': 'object',
            'generate_rights': False,
            'format': 'standard',
            'search': [
                {
                    'type': 'in',
                    'fields': [ '_uuid' ],
                    'in': [ uuid ]
                }
            ]
        }
        response = self.easydb_context.search('user', 'oai_pmh', query)
        if (len(response['objects']) == 0):
            return None
        js = response['objects'][0]
        import json
        record = Record(self, uuid)
        collections = get_json_value(js, '_collections')
        if collections is not None:
            for collection in collections:
                # TODO: setSpec
                record.collections.append(collection['_id'])
        objecttype = get_json_value(js, '_objecttype', True)
        pool_path = get_json_value(js, '{}._pool._path'.format(objecttype))
        if pool_path is not None:
            record.set_specs.append(self.get_spec(pool_path, 'pool'))
        return record

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

class Record(object):
    def __init__(self, repository, uuid):
        self.uuid = uuid
        self.identifier = 'oai:{}:{}'.format(repository.namespace_identifier, uuid)
        self.collections = []
        self.set_specs = []

