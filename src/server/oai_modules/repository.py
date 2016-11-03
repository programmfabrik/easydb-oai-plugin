# -*- coding: utf-8 -*-

import oai_modules.request
import oai_modules.response

class Repository(object):
    def __init__(self, base_url, name, admin_email):
        self.base_url = base_url
        self.name = name
        self.admin_email = admin_email
    def process_request(self, parameters):
        try:
            request = oai_modules.request.Request.parse(self, parameters)
            return request.process()
        except oai_modules.request.ParseError as e:
            request = oai_modules.request.Request(self)
            return oai_modules.response.ErrorResponse(request, e.error_code, e.error_message)
