# -*- coding: utf-8 -*-

import oai_modules.response

class Request(object):
    def __init__(self, repository, verb=None, parameters={}):
        self.repository = repository
        self.verb = verb
        self.parameters = parameters
    def process(self):
        raise NotImplementedError
    @staticmethod
    def parse(repository, parameters):
        verb = None
        for key, values in parameters.items():
            if key == 'verb':
                if len(values) > 1:
                    raise ParseError('badArgument', 'more than one verbs')
                verb = values[0]
        if verb is None:
            raise ParseError('badVerb', 'verb argument is missing')
        if verb not in oai_requests:
            raise ParseError('badVerb', 'wrong verb')
        return oai_requests[verb](repository, parameters)
    def _get_parameter(self, name, required=True):
        if name not in self.parameters or len(self.parameters[name]) == 0:
            if required:
                raise ParseError('badArgument', '{} parameter is missing'.format(name))
            else:
                return None
        return self.parameters[name][0]
    def _parse_identifier(self, identifier):
        identifier_parts = identifier.split(':')
        if len(identifier_parts) != 3:
            raise ParseError('idDoesNotExist', 'wrong identifier format')
        if identifier_parts[0] != 'oai':
            raise ParseError('idDoesNotExist', 'wrong identifier scheme')
        if identifier_parts[1] != self.repository.namespace_identifier:
            raise ParseError('idDoesNotExist', 'wrong identifier namespace')
        if len(identifier_parts[2]) == 0:
            raise ParseError('idDoesNotExist', 'wrong identifier')
        return identifier_parts[2]
    def _parse_set_filter(self, set_spec):
        set_parts = set_spec.split(':')
        if len(set_parts) == 1:
            raise ParseError('badArgument', 'wrong set')
        filter_type = set_parts[0]
        filter_id = set_parts[-1]
        if filter_type not in ('pool', 'collection', 'objecttype'):
            raise ParseError('badArgument', 'wrong set')
        try:
            if filter_type != 'objecttype':
                filter_id = int(filter_id)
            return (filter_type, filter_id)
        except ValueError:
            raise ParseError('badArgument', 'wrong set')
    def _get_metadata_format(self, required=True):
        prefix = self._get_parameter('metadataPrefix', required)
        if prefix is not None:
            for f in self.repository.get_metadata_formats():
                if f.prefix == prefix:
                    return f
            else:
                raise ParseError('cannotDisseminateFormat')

class Identify(Request):
    def __init__(self, repository, parameters={}):
        super(Identify, self).__init__(repository, 'Identify', parameters)
    def process(self):
        earliest_datestamp = self.repository.get_earliest_datestamp()
        return oai_modules.response.Identify(self, earliest_datestamp)

class ListMetadataFormats(Request):
    def __init__(self, repository, parameters={}):
        super(ListMetadataFormats, self).__init__(repository, 'ListMetadataFormats', parameters)
    def process(self):
        return oai_modules.response.ListMetadataFormats(self, self.repository.get_metadata_formats())

class ListSets(Request):
    def __init__(self, repository, parameters={}):
        super(ListSets, self).__init__(repository, 'ListSets', parameters)
        self.resumption_token = self._get_parameter('resumptionToken', False)
    def process(self):
        result = self.repository.get_sets(self.resumption_token)
        if result is None:
            return oai_modules.response.Error(self, 'badResumptionToken')
        sets, resumption_token = result
        return oai_modules.response.ListSets(self, sets, resumption_token)

class ListIdentifiers(Request):
    def __init__(self, repository, parameters={}):
        super(ListIdentifiers, self).__init__(repository, 'ListIdentifiers', parameters)
        set_param = self._get_parameter('set', required=False)
        self.set_filter = self._parse_set_filter(set_param) if set_param is not None else None
        self.resumption_token = self._get_parameter('resumptionToken', False)
        self.metadata_format = self._get_metadata_format(self.resumption_token is None)
        self.from_filter = self._get_parameter('from', False)
        self.until_filter = self._get_parameter('until', False)
    def process(self):
        result = self.repository.get_records(self.metadata_format, True, self.resumption_token, self.from_filter, self.until_filter, self.set_filter)
        if result is None:
            return oai_modules.response.Error(self, 'badResumptionToken')
        records, new_resumption_token = result
        if len(records) == 0:
            return oai_modules.response.Error(self, 'noIdentifiersMatch')
        return oai_modules.response.Records(self, records, self.metadata_format, new_resumption_token)

class ListRecords(Request):
    def __init__(self, repository, parameters={}):
        super(ListRecords, self).__init__(repository, 'ListRecords', parameters)
        set_param = self._get_parameter('set', required=False)
        self.set_filter = self._parse_set_filter(set_param) if set_param is not None else None
        self.resumption_token = self._get_parameter('resumptionToken', False)
        self.metadata_format = self._get_metadata_format()
        self.from_filter = self._get_parameter('from', False)
        self.until_filter = self._get_parameter('until', False)
    def process(self):
        result = self.repository.get_records(self.metadata_format, False, self.resumption_token, self.from_filter, self.until_filter, self.set_filter)
        if result is None:
            return oai_modules.response.Error(self, 'badResumptionToken')
        records, new_resumption_token = result
        if records is None:
            return oai_modules.response.Error(self, 'badResumptionToken')
        if len(records) == 0:
            return oai_modules.response.Error(self, 'noIdentifiersMatch')
        return oai_modules.response.Records(self, records, self.metadata_format, new_resumption_token)

class GetRecord(Request):
    def __init__(self, repository, parameters={}):
        super(GetRecord, self).__init__(repository, 'GetRecord', parameters)
        self.uuid = self._parse_identifier(self._get_parameter('identifier'))
        self.metadata_format = self._get_metadata_format()
    def process(self):
        record = self.repository.get_record(self.uuid, self.metadata_format)
        if record is None:
            return oai_modules.response.Error(self, 'idDoesNotExist')
        return oai_modules.response.Records(self, [record], self.metadata_format)

class ParseError(Exception):
    def __init__(self, error_code, error_message=None):
        self.error_code = error_code
        self.error_message = error_message

oai_requests = {
    'Identify': Identify,
    'ListSets': ListSets,
    'ListMetadataFormats': ListMetadataFormats,
    'GetRecord': GetRecord,
    'ListRecords': ListRecords,
    'ListIdentifiers': ListIdentifiers
}
