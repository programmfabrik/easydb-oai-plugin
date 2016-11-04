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
    def _get_parameter(self, name):
        if name not in self.parameters:
            raise ParseError('badArgument', '{} parameter is missing'.format(name))
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

class Identify(Request):
    def __init__(self, repository, parameters={}):
        super(Identify, self).__init__(repository, 'Identify', parameters)
    def process(self):
        return oai_modules.response.Identify(self)

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
        return oai_modules.response.ListMetadataFormats(self)

class GetRecord(Request):
    def __init__(self, repository, parameters={}):
        super(GetRecord, self).__init__(repository, 'GetRecord', parameters)
        self.uuid = self._parse_identifier(self._get_parameter('identifier'))
        self.metadataPrefix = self._get_parameter('metadataPrefix')
    def process(self):
        record = self.repository.get_record(self.uuid)
        if record is None:
            raise ParseError('idDoesNotExist')
        return oai_modules.response.GetRecord(self, record)

class ParseError(Exception):
    def __init__(self, error_code, error_message=None):
        self.error_code = error_code
        self.error_message = error_message

oai_requests = {
    'Identify': Identify,
    'ListSets': ListSets,
    'ListMetadataFormats': ListMetadataFormats,
    'GetRecord': GetRecord
}
