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

class ParseError(Exception):
    def __init__(self, error_code, error_message=None):
        self.error_code = error_code
        self.error_message = error_message

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

oai_requests = {
    'Identify': Identify,
    'ListSets': ListSets
}
