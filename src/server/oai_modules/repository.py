# -*- coding: utf-8 -*-

import context
import oai_modules.request
import oai_modules.response
import oai_modules.util
import oai_modules.set
import oai_modules.record
import datetime

class Repository(object):
    def __init__(self, easydb_context, base_url, name, namespace_identifier, admin_email, metadata_formats, tagfilter_sets_js, include_eas_urls):
        self.easydb_context = easydb_context
        self.base_url = base_url
        self.name = name
        self.namespace_identifier = namespace_identifier
        self.admin_email = admin_email
        self.metadata_formats = metadata_formats
        self.include_eas_urls = include_eas_urls
        self.tagfilter_set_names = []
        self.tagfilter_sets = {}
        if tagfilter_sets_js is not None:
            for tagfilter_set_js in tagfilter_sets_js:
                set_name = tagfilter_set_js['set_name']
                tagfilter = tagfilter_set_js['tagfilter']
                self.tagfilter_set_names.append(set_name)
                self.tagfilter_sets[set_name] = tagfilter
    def process_request(self, parameters):
        try:
            request = oai_modules.request.Request.parse(self, parameters)
            return request.process()
        except oai_modules.util.ParseError as e:
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
            MetadataFormat('dc', 'oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
            MetadataFormat('standard', 'easydb', '', ''),
         ]
        return out_of_the_box + self.metadata_formats
    def get_sets(self, resumption_token, limit=100):
        return oai_modules.set.SetManager(self).get_sets(resumption_token, limit)
    def get_record(self, uuid, metadata_format):
        return oai_modules.record.RecordManager(self).get_record(uuid, metadata_format)
    def get_records(self, metadata_format, only_headers, resumption_token, range_from, range_until, set_type, set_id, limit=100):
        return oai_modules.record.RecordManager(self).get_records(metadata_format, only_headers, resumption_token, range_from, range_until, set_type, set_id, limit)

class MetadataFormat(object):
    def __init__(self, ftype, prefix, schema, namespace):
        self.ftype = ftype
        self.prefix = prefix
        self.schema = schema
        self.namespace = namespace


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

