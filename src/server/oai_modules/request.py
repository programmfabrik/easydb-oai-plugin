# -*- coding: utf-8 -*-

import oai_modules.response

class OAIRequest(object):
    def __init__(self, repository, parameters={}):
        self.repository = repository
        self.parameters = parameters
    def process(self):
        return oai_modules.response.OAIResponse(self)

