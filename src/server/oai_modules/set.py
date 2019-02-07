# -*- coding: utf-8 -*-

import oai_modules.util
import json


class SetManager(object):

    def __init__(self, repository):
        self.repository = repository

    def get_sets(self, resumption_token, limit):
        scroll_info = ScrollInfo.parse(resumption_token)
        sets = []
        pool_sets = []
        objecttypes_with_pools = []
        new_resumption_token = None

        for set_type in set_types:
            if scroll_info.set_type is not None and scroll_info.set_type != set_type:
                continue
            search_limit = limit - len(sets)
            has_more = self._extend_sets(
                sets, set_type, scroll_info.offset, search_limit, pool_sets, objecttypes_with_pools)
            if has_more:
                scroll_info.offset += search_limit
                scroll_info.set_type = set_type
                new_resumption_token = str(scroll_info)
                break
            scroll_info.set_type = None
            scroll_info.offset = 0

        for p in pool_sets:
            sets += [Set(u'{} in {}'.format(ot, p[0]), u'objecttype_pool:{}:{}'.format(ot, p[1]))
                     for ot in objecttypes_with_pools]

        return (sets, new_resumption_token)

    def _extend_sets(self, all_sets, set_type, offset, limit, pool_sets, objecttypes_with_pools):
        if set_type == 'objecttype':
            sets, total_count = self._get_objecttypes(
                offset, limit, objecttypes_with_pools)
        elif set_type == 'tagfilter':
            sets, total_count = self._get_tagfilters(offset, limit)
        else:
            sets, total_count = self._search_sets(
                set_type, offset, limit, pool_sets)
        all_sets += sets

        return total_count > offset + limit

    def _get_objecttypes(self, offset, limit, objecttypes_with_pools):
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
                    objecttypes_with_pools.append(t['name'])

        table_count = len(table_names)
        offset = min(offset, table_count - 1)
        limit = min(limit, table_count - offset)
        table_names = table_names[offset:offset+limit]
        sets = [Set(tn, u'objecttype:{}'.format(tn)) for tn in table_names]

        return sets, table_count

    def _get_tagfilters(self, offset, limit):
        set_count = len(self.repository.tagfilter_set_names)
        offset = min(offset, set_count - 1)
        limit = min(limit, set_count - offset)
        set_names = self.repository.tagfilter_set_names[offset:offset+limit]
        sets = [Set(tn, u'tagfilter:{}'.format(tn)) for tn in set_names]

        return sets, set_count

    def _search_sets(self, base_type, offset, limit, pool_sets):
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

        response = self.repository.easydb_context.search(
            'user', 'oai_pmh', query)
        language = response['language']
        sets = []

        for obj in response['objects']:
            spec = self._get_spec(obj['_path'], base_type)
            names = []
            for i in range(len(obj['_path'])):
                if language in obj['_path'][i][base_type][set_names[base_type]['objkey']]:
                    names.append(obj['_path'][i][base_type]
                                 [set_names[base_type]['objkey']][language])
                else:
                    names.append(spec)
            set_name = " / ".join(names[1 if len(names) > 1 else 0:])
            if base_type == 'pool':
                pool_sets.append((set_name, spec))
            sets.append(Set(set_name, spec))
        return (sets, response['count'])

    def _get_spec(self, path_js, base_type):
        return ':'.join([base_type] + list(map(lambda element: str(element[base_type]['_id']), path_js)))


class ScrollInfo(object):

    def __init__(self, set_type, offset):
        self.set_type = set_type
        self.offset = offset

    def __str__(self):
        info = {
            's': self.set_type,
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

        return ScrollInfo(info['s'], info['o'])


class Set(object):

    def __init__(self, name, spec):
        self.name = name
        self.spec = spec


set_types = [
    'objecttype',
    'pool',
    'collection',
    'tagfilter'
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
