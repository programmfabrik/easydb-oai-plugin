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
        for key, value in parameters.items():
            if value is None:
                raise ParseError('badArgument', 'query string parameter {} has no value'.format(key))
            if key == 'verb':
                verb = value
        if verb is None:
            raise ParseError('badVerb', 'verb argument is missing')
        if verb not in oai_requests:
            raise ParseError('badVerb', 'wrong verb')
        return oai_requests[verb](repository, parameters)
    def _get_parameter(self, name, required=True):
        if name not in self.parameters:
            if required:
                raise ParseError('badArgument', '{} parameter is missing'.format(name))
            else:
                return None
        return self.parameters[name]
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
        if set_parts[0] not in ('pool', 'collection'):
            raise ParseError('badArgument', 'wrong set')
        try:
            return (set_parts[0], int(set_parts[-1]))
        except ValueError:
            raise ParseError('badArgument', 'wrong set')

class Identify(Request):
    def __init__(self, repository, parameters={}):
        super(Identify, self).__init__(repository, 'Identify', parameters)
    def process(self):
        earliest_datestamp = self.repository.get_earliest_datestamp()
        return oai_modules.response.Identify(self, earliest_datestamp)

class ListSets(Request):
    def __init__(self, repository, parameters={}):
        super(ListSets, self).__init__(repository, 'ListSets', parameters)
    def process(self):
        sets = self.repository.get_sets()
        return oai_modules.response.ListSets(self, sets)

class ListMetadataFormats(Request):
    def __init__(self, repository, parameters={}):
        super(ListMetadataFormats, self).__init__(repository, 'ListMetadataFormats', parameters)
    def process(self):
        return oai_modules.response.ListMetadataFormats(self, self.repository.get_metadata_formats())

class GetRecord(Request):
    def __init__(self, repository, parameters={}):
        super(GetRecord, self).__init__(repository, 'GetRecord', parameters)
        self.uuid = self._parse_identifier(self._get_parameter('identifier'))
        self.metadataPrefix = self._get_parameter('metadataPrefix')
    def process(self):
        record = self.repository.get_record(self.uuid)
        if record is None:
            return oai_modules.response.Error(self, 'idDoesNotExist')
        return oai_modules.response.Records(self, [record])

class ListRecords(Request):
    def __init__(self, repository, parameters={}):
        super(ListRecords, self).__init__(repository, 'ListRecords', parameters)
        set_param = self._get_parameter('set', required=False)
        self.set_filter = self._parse_set_filter(set_param) if set_param is not None else None
        self.metadataPrefix = self._get_parameter('metadataPrefix')
    def process(self):
        records = self.repository.get_records(self.set_filter)
        if len(records) == 0:
            return oai_modules.response.Error(self, 'noRecordsMatch')
        return oai_modules.response.Records(self, records)

class ListIdentifiers(Request):
    def __init__(self, repository, parameters={}):
        super(ListIdentifiers, self).__init__(repository, 'ListIdentifiers', parameters)
        set_param = self._get_parameter('set', required=False)
        self.set_filter = self._parse_set_filter(set_param) if set_param is not None else None
        self.metadataPrefix = self._get_parameter('metadataPrefix')
    def process(self):
        records = self.repository.get_records(self.set_filter)
        if len(records) == 0:
            return oai_modules.response.Error(self, 'noIdentifiersMatch')
        return oai_modules.response.Records(self, records, only_header=True)

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
