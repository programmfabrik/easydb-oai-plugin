# -*- coding: utf-8 -*-

import context
import oai_modules.request
import oai_modules.response
import oai_modules.util
import datetime

class Repository(object):
    def __init__(self, easydb_context, base_url, name, namespace_identifier, admin_email, metadata_formats):
        self.easydb_context = easydb_context
        self.base_url = base_url
        self.name = name
        self.namespace_identifier = namespace_identifier
        self.admin_email = admin_email
        self.metadata_formats = metadata_formats
    def process_request(self, parameters):
        try:
            request = oai_modules.request.Request.parse(self, parameters)
            return request.process()
        except oai_modules.request.ParseError as e:
            request = oai_modules.request.Request(self)
            return oai_modules.response.Error(request, e.error_code, e.error_message)
    def get_earliest_datestamp(self):
        db_cursor = self.easydb_context.get_db_cursor()
        db_cursor.execute(sql_get_earliest_datestamp_from_config)
        if db_cursor.rowcount > 0:
            return db_cursor.fetchone()['earliest_datestamp']
        db_cursor.execute(sql_get_earliest_datestamp_from_history)
        if db_cursor.rowcount < 1:
            return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        earliest_datestamp = db_cursor.fetchone()['earliest_datestamp']
        db_cursor.execute(sql_insert_earliest_datestamp_into_config.format(earliest_datestamp))
        return earliest_datestamp
    def get_metadata_formats(self):
        out_of_the_box = [
            MetadataFormat('oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
            MetadataFormat('easydb_standard', '', ''),
            MetadataFormat('easydb_flat', '', '')
         ]
        return out_of_the_box + self.metadata_formats
    def get_sets(self, resumption_token, limit=100):
        base_types = ['objecttype', 'pool', 'collection']
        rt_set = None
        offset = 0
        if resumption_token is not None:
            parts = resumption_token.split(':')
            if len(parts) != 2:
                return None
            rt_set = parts[0]
            if parts[0] not in set_names.keys():
                return None
            try:
                offset = int(parts[1])
            except ValueError:
                return None
        sets = []
        new_resumption_token = None
        for base_type in base_types:
            if rt_set is not None and rt_set != base_type:
                continue
            search_limit = limit - len(sets)
            has_more = self._extend_sets(sets, base_type, offset, search_limit)
            if has_more:
                new_resumption_token = '{}:{}'.format(base_type, offset + search_limit)
                break
            rt_set = None
            offset = 0
        return (sets, new_resumption_token)
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
        return self._parse_record(response['objects'][0])
    def get_records(self, set_filter):
        search_elements = []
        if set_filter:
            filter_type, filter_id = set_filter
            if filter_type == 'pool':
                search_elements.append({'type': 'in', 'objecttype': '_pool', 'in': [filter_id]})
            else:
                search_elements.append({'type': 'in', 'fields': ['_collections._id'], 'in': [filter_id]})
        query = {
            'type': 'object',
            'generate_rights': False,
            'format': 'standard',
            'search': search_elements
        }
        response = self.easydb_context.search('user', 'oai_pmh', query)
        return [self._parse_record(object_js) for object_js in response['objects']]
    def _extend_sets(self, all_sets, base_type, offset, limit):
        if base_type == 'objecttype':
            sets, total_count = self._get_objecttypes(offset, limit)
        else:
            sets, total_count = self._search_sets(base_type, offset, limit)
        all_sets += sets
        return total_count > offset + limit
    def _get_objecttypes(self, offset, limit):
        datamodel = self.easydb_context.get_datamodel()
        table_names = [t['name'] for t in datamodel['user']['tables']]
        table_count = len(table_names)
        offset = min(offset, table_count - 1)
        limit = min(limit, table_count - offset)
        table_names = table_names[offset:limit]
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
        response = self.easydb_context.search('user', 'oai_pmh', query)
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
    def _parse_record(self, object_js):
        try:
            uuid = context.get_json_value(object_js, '_uuid', True)
            record = Record(self, uuid)
            objecttype = context.get_json_value(object_js, '_objecttype', True)
            pool_path = context.get_json_value(object_js, '{}._pool._path'.format(objecttype))
            if pool_path is not None:
                record.set_specs.append(self._get_spec(pool_path, 'pool'))
            return record
        except context.EasydbException as e:
            raise oai_modules.util.InternalError(e.message)

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

class MetadataFormat(object):
    def __init__(self, prefix, schema, namespace):
        self.prefix = prefix
        self.schema = schema
        self.namespace = namespace

class Set(object):
    def __init__(self, name, spec):
        self.name = name
        self.spec = spec

class Record(object):
    def __init__(self, repository, uuid):
        self.uuid = uuid
        self.identifier = 'oai:{}:{}'.format(repository.namespace_identifier, uuid)
        self.set_specs = []

sql_get_earliest_datestamp_from_config = """
select value_text as earliest_datestamp
from ez_config
where class = 'oai_pmh' and key = 'earliest_datestamp'
"""

sql_get_earliest_datestamp_from_history = """
select replace(to_char(min("time:created") at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z' as earliest_datestamp
from "ez_objects:history";
"""

sql_insert_earliest_datestamp_into_config = """
insert into ez_config (class, key, value_text)
values ('oai_pmh', 'earliest_datestamp', '{}')
"""

