# -*- coding: utf-8 -*-

import oai_modules.util

class SetManager(object):
    def __init__(self, repository):
        self.repository = repository
    def get_sets(self, resumption_token, limit):
        scroll_info = ScrollInfo.parse(resumption_token)
        sets = []
        new_resumption_token = None
        for base_type in base_types:
            if scroll_info.base_type is not None and scroll_info.base_type != base_type:
                continue
            search_limit = limit - len(sets)
            has_more = self._extend_sets(sets, base_type, scroll_info.offset, search_limit)
            if has_more:
                scroll_info.offset += search_limit
                scroll_info.base_type = base_type
                new_resumption_token = str(scroll_info)
                break
            scroll_info.base_type = None
            scroll_info.offset = 0
        return (sets, new_resumption_token)
    def _extend_sets(self, all_sets, base_type, offset, limit):
        if base_type == 'objecttype':
            sets, total_count = self._get_objecttypes(offset, limit)
        else:
            sets, total_count = self._search_sets(base_type, offset, limit)
        all_sets += sets
        return total_count > offset + limit
    def _get_objecttypes(self, offset, limit):
        datamodel = self.repository.easydb_context.get_datamodel()
        table_names = [t['name'] for t in datamodel['user']['tables']]
        table_count = len(table_names)
        offset = min(offset, table_count - 1)
        limit = min(limit, table_count - offset)
        table_names = table_names[offset:offset+limit]
        sets = [Set(tn, 'objecttype:{}'.format(tn)) for tn in table_names]
        return sets, table_count
    def _search_sets(self, base_type, offset, limit):
        query = {
            'type': base_type,
            'generate_rights': False,
            'offset': offset,
            'limit': limit,
            'sort': [
                {
                    'field': '{}._id'.format(base_type)
                }
            ]
        }
        response = self.repository.easydb_context.search('user', 'oai_pmh', query)
        language = response['language']
        has_more = response["count"] > len(response['objects'])
        sets = []
        for obj in response['objects']:
            spec = self._get_spec(obj['_path'], base_type)
            name = obj['_path'][-1][base_type][set_names[base_type]['objkey']][language]
            sets.append(Set(name, spec))
        return (sets, response['count'])
    def _get_spec(self, path_js, base_type):
        return ':'.join([base_type] + list(map(lambda element: str(element[base_type]['_id']), path_js)))

class ScrollInfo(object):
    def __init__(self, base_type, offset):
        self.base_type = base_type
        self.offset = offset
    def __str__(self):
        info = {
            'b': self.base_type,
            'o': self.offset
        }
        return oai_modules.util.tokenize(info)
    @staticmethod
    def parse(token):
        if token is None:
            return ScrollInfo(None, 0)
        try:
            info = oai_modules.util.untokenize(token)
        except Exception:
            raise oai_modules.util.ParseError('badResumptionToken')
        return ScrollInfo(info['b'], info['o'])

class Set(object):
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec

base_types = [
    'objecttype',
    'pool',
    'collection'
]

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
