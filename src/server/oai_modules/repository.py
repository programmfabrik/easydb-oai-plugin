# -*- coding: utf-8 -*-

import oai_modules.request
import oai_modules.response

class OAIRepository(object):
    def __init__(self, base_url):
        self.base_url = base_url
    def process_request(self, parameters):
        for key, value in parameters.items():
            if value is None:
                return self.oai_bad_argument('query string parameter "{}" has no value'.format(key))
        request = oai_modules.request.OAIRequest(self, parameters)
        return request.process()
    def oai_bad_argument(self, message):
        request = oai_modules.request.OAIRequest(self)
        return oai_modules.response.OAIErrorResponse(request, 'badArgument', message)
