# -*- coding: utf-8 -*-

import oai_modules.util
import json
from context import get_json_value

class SetManager(object):

    def __init__(self, repository):
        self.repository = repository
        self.objecttypes_with_pools = []

    def get_sets(self, resumption_token, limit):
        if limit < 1:
            return ([], None)

        scroll_info = ScrollInfo.parse(resumption_token)
        sets = []
        pool_sets = []

        new_resumption_token = None
        offset = scroll_info.offset

        self._get_objecttypes()

        for set_type in set_types:
            self._extend_sets(sets, set_type, pool_sets)

        if offset + limit < len(sets):
            scroll_info.offset += limit
            new_resumption_token = str(scroll_info)

        return (sets[offset: offset + limit], new_resumption_token)

    def _extend_sets(self, all_sets, set_type, pool_sets):
        if set_type == 'tagfilter':
            sets = self._get_tagfilters()
        else:
            sets = self._search_sets(set_type, pool_sets)
        all_sets += sets

    def _get_objecttypes(self):
        datamodel = self.repository.easydb_context.get_datamodel(
            show_easy_pool_link=True,
            show_has_easy_owning_tables=True)
        table_names = []
        for t in datamodel['user']['tables']:
            if 'has_easy_owning_tables' in t:
                if isinstance(t['has_easy_owning_tables'], bool) and t['has_easy_owning_tables'] == True:
                    continue
            table_names.append(t['name'])
            if 'easy_pool_link' in t:
                if isinstance(t['easy_pool_link'], bool) and t['easy_pool_link'] == True:
                    self.objecttypes_with_pools.append(t['name'])

    def _get_tagfilters(self):
        set_names = self.repository.tagfilter_set_names
        sets = [Set(tn, u'tagfilter:{}'.format(tn)) for tn in set_names]

        return sets

    def _search_sets(self, base_type, pool_sets):

        query = {
            'limit': 0,
            'aggregations': {
                '_pools': {
                    'type': 'linked_object',
                    'field': '_pool',
                    'limit': 100000,
                    'sort': 'term'
                }
            }
        } if base_type == 'pool' else {
            'type': base_type,
            'generate_rights': False,
            'offset': 0,
            'sort': [
                {
                    'field': '{}._id'.format(base_type)
                }
            ]
        }

        response = self.repository.easydb_context.search(
            'user', 'oai_pmh', query)
        language = response['language']
        sets = []

        if base_type == 'pool':
            sets += self._search_pools_objecttypes(response['aggregations']['_pools']['linked_objects'], language)
        else:
            for obj in response['objects']:
                spec = ':'.join([base_type] + list(map(lambda element: str(element[base_type]['_id']), obj['_path'])))
                try:
                    set_name = " / ".join(list(map(lambda element: str(element[base_type][set_names[base_type]['objkey']][language]), obj['_path'])))
                except:
                    set_name = spec
                sets.append(Set(set_name, spec))

        return sets

    def _search_pools_objecttypes(self, pools, language):
        _sets = []
        _pools = {}
        not_empty_objecttypes_with_pools = []

        # add non-empty pools to the set list
        for p in pools:
            spec = 'pool:' + ':'.join(list(map(lambda element: str(element['_id']), p['_path'])))
            try:
                set_name = " / ".join(list(map(lambda element: str(element['text']), p['_path'])))
            except:
                set_name = spec
            _sets.append(Set(set_name, spec))

            _pool_id = get_json_value(p, '_id')
            if _pool_id is not None:
                _pools[_pool_id] = [set_name, spec]

        # add objecttypes to the set list
        query = {
            'search': [],
            'limit': 0,
            'aggregations': {
                '_objecttype': {
                    'limit': 1000000,
                    'type': 'term',
                    'field': '_objecttype'
                }
            },
            'generate_rights': False
        }
        response = self.repository.easydb_context.search('user', 'oai_pmh', query)

        _terms = get_json_value(response, 'aggregations._objecttype.terms')
        if _terms is None:
            return _sets

        for _term in _terms:
            _ot = get_json_value(_term, 'term')
            if _ot is None:
                continue
            _sets.append(Set(_ot, 'objecttype:%s' % _ot))

            if _ot in self.objecttypes_with_pools:
                not_empty_objecttypes_with_pools.append(_ot)

        if len(not_empty_objecttypes_with_pools) < 1:
            return _sets

        # add combinations of pools and objecttypes to the set list
        query = {
            'search': [],
            'limit': 0,
            'aggregations': {},
            'objecttypes': not_empty_objecttypes_with_pools,
            'generate_rights': False
        }
        for _ot in not_empty_objecttypes_with_pools:
            query['aggregations'][_ot] = {
                'type': 'term',
                'field': '%s._pool.pool._id' % _ot,
                'sort': 'term',
                'limit': 1000000
            }
        response = self.repository.easydb_context.search('user', 'oai_pmh', query)

        for _ot in not_empty_objecttypes_with_pools:
            _terms = get_json_value(response, 'aggregations.%s.terms' % _ot)
            if _terms is None:
                continue

            for _term in _terms:
                _pool_id = get_json_value(_term, 'term')
                if _pool_id is None:
                    continue
                if not _pool_id in _pools:
                    continue

                _sets.append(Set(
                    '%s in %s' % (_ot, _pools[_pool_id][0]),
                    'objecttype_pool:%s:%s' % (_ot, _pools[_pool_id][1])
                ))

        return _sets


class ScrollInfo(object):

    def __init__(self, offset):
        self.offset = offset

    def __str__(self):
        info = {
            'o': self.offset
        }
        return oai_modules.util.tokenize(info)

    @staticmethod
    def parse(token):
        if token is None:
            return ScrollInfo(0)
        try:
            return ScrollInfo(oai_modules.util.untokenize(token)['o'])
        except Exception:
            raise oai_modules.util.ParseError('badResumptionToken')

        return None

class Set(object):

    def __init__(self, name, spec, description=None):
        self.name = name
        self.spec = spec
        self.description = description


set_types = [
    'pool',
    'collection',
    'tagfilter'
]

set_names = {
    'pool': {
        'objkey': 'name'
    },
    'collection': {
        'objkey': 'displayname'
    }
}
