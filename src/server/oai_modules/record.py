# -*- coding: utf-8 -*-

import context
import json
import oai_modules.util
import xml.etree.ElementTree as ET


class RecordManager(object):
    def __init__(self, repository):
        self.repository = repository

    def get_record(self, uuid, metadata_format):
        # deleted
        # db_cursor = self.repository.easydb_context.get_db_cursor()
        # db_cursor.execute(sql_get_deleted_record.format(uuid))
        # if db_cursor.rowcount > 0:
        #     record = Record(self.repository, uuid)
        #     record.deleted = True
        #     record.last_modified = db_cursor.fetchone()['datestamp']
        #     return record
        # existing
        query = {
            'type': 'object',
            'generate_rights': False,
            'format': 'long',
            'search': [
                {
                    'type': 'in',
                    'fields': ['_uuid'],
                    'in': [uuid]
                }
            ]
        }
        response = self.repository.easydb_context.search(
            'user', self.repository.username, query, self.repository.include_eas_urls)
        if (len(response['objects']) == 0):
            return None

        user_id = response['_user_id']
        language = response['language']
        metadata_info = MetadataInfo(user_id, language, metadata_format)

        return self._parse_records(response['objects'], metadata_info)[0]

    def get_records(self, metadata_format, only_headers, resumption_token, range_from, range_until, set_type, set_id, limit=100):

        try:
            scroll_info = ScrollInfo.parse(
                resumption_token,
                range_from,
                range_until,
                set_type,
                set_id,
                metadata_format.prefix if metadata_format is not None else None
            )
        except TypeError as e:
            return oai_modules.response.Error(self, 'badResumptionToken')

        for f in self.repository.get_metadata_formats():
            if scroll_info.metadata_prefix == f.prefix:
                metadata_format = f
                break
        else:
            raise oai_modules.util.InternalError(
                'metadata format {} not found'.format(scroll_info.metadata_prefix))
        search_elements = []
        objecttypes = []

        if scroll_info.set_type is not None:
            if scroll_info.set_type == 'pool':
                search_elements.append(
                    {
                        'type': 'in',
                        'objecttype': '_pool',
                        'in': [scroll_info.set_id]
                    }
                )

            elif scroll_info.set_type == 'objecttype_pool':
                try:
                    search_elements.append(
                        {
                            'type': 'in',
                            'objecttype': '_pool',
                            'in': [scroll_info.set_id[0]]
                        }
                    )
                    objecttypes.append(scroll_info.set_id[1])
                except Exception as e:
                    raise oai_modules.util.ParseError(
                        'badArgument', 'wrong set: '+str(e))

            elif scroll_info.set_type == 'collection':
                search_elements.append(
                    {
                        'type': 'in',
                        'fields': ['_collections._id'],
                        'in': [scroll_info.set_id]
                    }
                )

            elif scroll_info.set_type == 'tagfilter':
                if scroll_info.set_id not in self.repository.tagfilter_sets:
                    raise oai_modules.util.ParseError(
                        'badArgument', 'wrong set')

                search_elements.extend(self._tagfilter_to_search_elements(
                    self.repository.tagfilter_sets[scroll_info.set_id]))

            else:
                objecttypes.append(scroll_info.set_id)

        if scroll_info.range_from is not None or scroll_info.range_until is not None:
            search_element = {
                'type': 'range',
                'field': '_last_modified'
            }

            if scroll_info.range_from is not None:
                if not self.repository.datatime_format_ok(scroll_info.range_from):
                    raise oai_modules.util.ParseError('badArgument', 'from: ' + scroll_info.range_from)
                search_element['from'] = scroll_info.range_from

            if scroll_info.range_until is not None:
                if not self.repository.datatime_format_ok(scroll_info.range_until):
                    raise oai_modules.util.ParseError('badArgument', 'until: ' + scroll_info.range_until)
                search_element['to'] = scroll_info.range_until

            search_elements.append(search_element)

        query = {
            'type': 'object',
            'generate_rights': False,
            'objecttypes': objecttypes,
            'offset': scroll_info.offset,
            'limit': limit,
            'sort': [
                {
                    'field': '_system_object_id'
                }
            ],
            'format': 'long',
            'search': search_elements
        }
        response = self.repository.easydb_context.search('user', self.repository.username, query)
        user_id = response['_user_id']
        language = response['language']

        if only_headers:
            metadata_info = None
        else:
            metadata_info = MetadataInfo(user_id, language, metadata_format)

        records = self._parse_records(response['objects'], metadata_info)

        if response['count'] > scroll_info.offset + limit:
            scroll_info.offset += limit
            new_resumption_token = str(scroll_info)
        else:
            new_resumption_token = None

        return (records, new_resumption_token, metadata_format)

    def _parse_records(self, object_js, metadata_info=None):
        records = []
        xmldocs = []

        if metadata_info:
            export_result = self.repository.easydb_context.export_objects_as_xml(
                object_js,
                metadata_info.mdformat.ftype,
                metadata_info.mdformat.prefix,
                metadata_info.user_id,
                metadata_info.language,
                self.repository.merge_linked_objects,
                self.repository.merge_max_depth)
            xmldocs = context.get_json_value(export_result, 'documents', True)

        idx = 0
        for obj in object_js:
            try:
                uuid = context.get_json_value(obj, '_uuid', True)
                record = Record(self.repository, uuid)
                sets = set()

                for _set in self._get_tagfilter_sets_for_object(context.get_json_value(obj, '_tags')):
                    sets.add(_set)

                if '_objecttype' in obj:
                    objecttype = obj['_objecttype']
                    sets.add('objecttype:' + objecttype)
                    pool = context.get_json_value(obj, objecttype + "._pool")

                    if pool is not None:
                        pool_path = [str(context.get_json_value(
                            p, "pool._id", True)) for p in pool['_path']]

                        if len(pool_path) > 0:
                            pool_set_spec = 'pool:' + ':'.join(pool_path)
                            sets.add(pool_set_spec)
                            sets.add('objecttype_pool:{}:{}'.format(
                                objecttype, pool_set_spec))

                if sets is not None:
                    record.set_specs = sets
                record.last_modified = context.get_json_value(
                    obj, '_last_modified')

                if record.last_modified is None:
                    record.last_modified = self.repository.get_earliest_datestamp()
                else:
                    record.last_modified = oai_modules.util.to_iso_utc_timestring(
                        record.last_modified)

                if metadata_info:
                    xml_string = ''
                    if idx < len(xmldocs):
                        xml_string = xmldocs[idx].encode('utf-8')

                    if len(xml_string) == 0:
                        xml_string = default_dc_response

                    record.metadata = ET.fromstring(xml_string)

                records.append(record)

            except ET.ParseError as pe:
                raise oai_modules.util.InternalError(
                    'could not format oai metadata: could not parse xml record: {}'.format(pe.message))

            except Exception as e:
                raise oai_modules.util.InternalError(
                    'could not parse record: {}'.format(e.message))

            idx += 1

        return records

    def _get_tagfilter_sets_for_object(self, tags_js):
        if tags_js is None:
            return []

        tag_ids = [int(tag_js['_id']) for tag_js in tags_js]
        sets = []

        for name, tagfilter in self.repository.tagfilter_sets.items():
            if self._eval_tagfilter(tagfilter, tag_ids):
                sets.append('tagfilter:{}'.format(name))

        return sets

    def _eval_tagfilter(self, tagfilter, tag_ids):
        for tf_type, tags in tagfilter.items():
            if not self._eval_tagfilter_part(tf_type, tags, tag_ids):
                return False

        return True

    def _eval_tagfilter_part(self, tf_type, tags, tag_ids):
        if tf_type == 'any':
            for tag in tags:
                if tag in tag_ids:
                    return True
            return False

        if tf_type == 'not':
            for tag in tags:
                if tag in tag_ids:
                    return False
            return True

        if tf_type == 'all':
            for tag in tags:
                if tag not in tag_ids:
                    return False
            return True

        return True

    def _tagfilter_to_search_elements(self, tagfilter):
        return [item for sublist in [self._tagfilter_part_to_search_elements(tf_type, tags) for tf_type, tags in tagfilter.items()] for item in sublist]

    def _tagfilter_part_to_search_elements(self, tf_type, tags):
        if tf_type == 'any':
            return [{'type': 'in', 'fields': ['_tags._id'], 'in': tags}]
        if tf_type == 'not':
            return [{'type': 'in', 'fields': ['_tags._id'], 'in': tags, 'bool': 'must_not'}]
        if tf_type == 'all':
            return [{'type': 'in', 'fields': ['_tags._id'], 'in': [tag]} for tag in tags]
        return []


class ScrollInfo(object):
    def __init__(self, range_from, range_until, set_type, set_id, offset, metadata_prefix):
        self.range_from = range_from
        self.range_until = range_until
        self.set_type = set_type
        self.set_id = set_id
        self.offset = offset
        self.metadata_prefix = metadata_prefix

    def __str__(self):
        info = {
            'f': self.range_from,
            'u': self.range_until,
            't': self.set_type,
            'i': self.set_id,
            'o': self.offset,
            'm': self.metadata_prefix
        }
        return oai_modules.util.tokenize(info)

    @staticmethod
    def parse(token, range_from, range_until, set_type, set_id, metadata_prefix):
        if token is None:
            return ScrollInfo(range_from, range_until, set_type, set_id, 0, metadata_prefix)

        try:
            info = oai_modules.util.untokenize(token)
            return ScrollInfo(info['f'], info['u'], info['t'], info['i'], info['o'], info['m'])
        except Exception:
            raise oai_modules.util.ParseError('badResumptionToken')

        return None


class Record(object):
    def __init__(self, repository, uuid):
        self.uuid = uuid
        self.identifier = 'oai:{}:{}'.format(
            repository.namespace_identifier, uuid)
        self.set_specs = []
        self.last_modified = None
        self.metadata = None
        self.deleted = False


class MetadataInfo(object):
    def __init__(self, user_id, language, mdformat):
        self.user_id = user_id
        self.language = language
        self.mdformat = mdformat


sql_get_deleted_record = """
select replace(to_char(d."time:created" at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS'), ' ', 'T') || 'Z' as datestamp
from "ez_objects:history" i
join "ez_objects:history" d on (i."ez_object_unique:id:152" = d."ez_object_unique:id:152" and d.":op" = 'DELETE')
where i.":op" = 'INSERT' and i."uuid:668" = '{}'
"""

default_dc_response = u"""<?xml version="1.0"?>
<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>NO OAI-DC PROFILE CONFIGURED</dc:title>
    <dc:creator/>
    <dc:subject/>
    <dc:description/>
    <dc:publisher/>
    <dc:contributor/>
    <dc:date/>
    <dc:type/>
    <dc:format/>
    <dc:identifier/>
    <dc:source/>
    <dc:language/>
    <dc:relation/>
    <dc:coverage/>
    <dc:rights/>
</oai_dc:dc>
"""
