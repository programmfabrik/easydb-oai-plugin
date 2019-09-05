# -*- coding: utf-8 -*-

import context
import oai_modules.request
import oai_modules.response
import oai_modules.util
import oai_modules.set
import oai_modules.record
import datetime


class Repository(object):
    def __init__(self, easydb_context, base_url, name, namespace_identifier, admin_email, metadata_formats, tagfilter_sets_js, include_eas_urls, merge_linked_objects, records_limit):
        self.easydb_context = easydb_context
        self.base_url = base_url
        self.name = name
        self.namespace_identifier = namespace_identifier
        self.admin_email = admin_email
        self.metadata_formats = metadata_formats
        self.include_eas_urls = include_eas_urls
        self.tagfilter_set_names = []
        self.tagfilter_sets = {}
        self.merge_linked_objects = merge_linked_objects
        self.records_limit = records_limit
        if tagfilter_sets_js is not None:
            for tagfilter_set_js in tagfilter_sets_js:
                set_name = tagfilter_set_js['set_name']
                tagfilter = tagfilter_set_js['tagfilter']
                self.tagfilter_set_names.append(set_name)
                self.tagfilter_sets[set_name] = tagfilter
        self.username = 'oai_pmh'
        try:
            db_cursor = easydb_context.get_db_cursor()
            db_cursor.execute("""SELECT frontend_language FROM ez_user WHERE login = '{0}'""".format(self.username))
            self.language = db_cursor.fetchone()['frontend_language']
        except:
            self.language = ''

    def process_request(self, parameters):
        try:
            request = oai_modules.request.Request.parse(self, parameters)
            return request.process()
        except oai_modules.util.ParseError as e:
            request = oai_modules.request.Request(self)
            return oai_modules.response.Error(request, e.error_code, e.error_message)

    def get_earliest_datestamp(self):
        db_cursor = self.easydb_context.get_db_cursor()

        earliest_datestamp = None
        save_datestamp_in_config = None

        db_cursor.execute(sql_get_earliest_datestamp_from_config)
        if db_cursor.rowcount > 0:
            earliest_datestamp = db_cursor.fetchone()['earliest_datestamp']
            if earliest_datestamp == 'None':
                earliest_datestamp = None
                save_datestamp_in_config = False # UPDATE
        else:
            save_datestamp_in_config = True # INSERT

        if earliest_datestamp is None:
            db_cursor.execute(sql_get_earliest_datestamp_from_history)
            if db_cursor.rowcount > 0:
                earliest_datestamp = db_cursor.fetchone()['earliest_datestamp']
                if earliest_datestamp == 'None':
                    earliest_datestamp = None

        if earliest_datestamp is None:
            earliest_datestamp = self.format_iso8601(datetime.datetime.utcnow())

        if save_datestamp_in_config is not None:
            if save_datestamp_in_config:
                db_cursor.execute(sql_insert_earliest_datestamp_into_config.format(earliest_datestamp))
            else:
                db_cursor.execute(sql_update_earliest_datestamp_in_config.format(earliest_datestamp))

        return earliest_datestamp

    def get_metadata_formats(self):
        out_of_the_box = [
            MetadataFormat('dc', 'oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
                           'http://www.openarchives.org/OAI/2.0/oai_dc/'),
            MetadataFormat('standard', 'easydb', 'https://schema.easydb.de/EASYDB/1.0/objects.xsd',
                           'https://schema.easydb.de/EASYDB/1.0/objects/'),
        ]
        return out_of_the_box + self.metadata_formats

    def get_sets(self, resumption_token):
        return oai_modules.set.SetManager(self).get_sets(resumption_token)

    def get_record(self, uuid, metadata_format):
        return oai_modules.record.RecordManager(self).get_record(uuid, metadata_format)

    def get_records(self, metadata_format, only_headers, resumption_token, range_from, range_until, set_type, set_id):
        return oai_modules.record.RecordManager(self).get_records(metadata_format, only_headers, resumption_token, range_from, range_until, set_type, set_id, self.records_limit)

    def format_iso8601(self, dateobj):
        return dateobj.strftime('%Y-%m-%dT%H:%M:%SZ')

    def parse_iso8601(self, datestring, short=False):
        if short:
            return datetime.datetime.strptime(datestring, '%Y-%m-%d')
        return datetime.datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%SZ')

    def datatime_format_ok(self, datestring):
        try:
            self.parse_iso8601(datestring)
            return True
        except:
            try:
                self.parse_iso8601(datestring, True)
                return True
            except:
                pass

        return False


class MetadataFormat(object):
    def __init__(self, ftype, prefix, schema, namespace):
        self.ftype = ftype
        self.prefix = prefix
        self.schema = schema
        self.namespace = namespace


sql_get_earliest_datestamp_from_config = """
SELECT
    value_text AS earliest_datestamp
FROM
    ez_config
WHERE
    class = 'oai_pmh'
    AND key = 'earliest_datestamp'
"""

sql_get_earliest_datestamp_from_history = """
SELECT
    replace(
        to_char(
            min("time:created") at time zone 'UTC',
            'YYYY-MM-DD HH24:MI:SS'
        ),
        ' ',
        'T'
    ) || 'Z' AS earliest_datestamp
FROM
    "ez_objects:history"
"""

sql_insert_earliest_datestamp_into_config = """
INSERT INTO
    ez_config (class, key, value_text)
VALUES
    ('oai_pmh', 'earliest_datestamp', '{}')
"""

sql_update_earliest_datestamp_in_config = """
UPDATE
    ez_config
SET
    value_text = '{}'
WHERE
    class = 'oai_pmh'
    AND key = 'earliest_datestamp'
"""
