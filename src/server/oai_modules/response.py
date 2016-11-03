# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime

class Response(object):
    def __init__(self, request):
        self.request = request
        self.error_code = None
    def __str__(self):
        oaipmh = ET.Element('OAI-PMH', oaipmh_attributes)
        responseDate = ET.SubElement(oaipmh, 'responseDate')
        responseDate.text = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        request = ET.SubElement(oaipmh, 'request')
        request.text = self.request.repository.base_url
        if self.error_code is not None:
            error = ET.SubElement(oaipmh, 'error', {'code': self.error_code})
            if self.error_message is not None:
                error.text = self.error_message
        else:
            request.attrib = self.request.parameters
            verb = ET.SubElement(oaipmh, self.request.verb)
            for item in self.get_response_items():
                se = ET.SubElement(verb, item.name)
                se.text = item.text
        return xml.dom.minidom.parseString(ET.tostring(oaipmh, 'UTF-8')).toprettyxml(indent='\t', encoding='UTF-8')
    def get_response_items(self):
        return []

class ErrorResponse(Response):
    def __init__(self, request, error_code, error_message=None):
        super(ErrorResponse, self).__init__(request)
        self.error_code = error_code
        self.error_message = error_message

class IdentifyResponse(Response):
    def __init__(self, request):
        super(IdentifyResponse, self).__init__(request)
    def get_response_items(self):
        return [
            ResponseItem('repositoryName', self.request.repository.name),
            ResponseItem('base_url', self.request.repository.base_url),
            ResponseItem('protocolVersion', '2.0'),
            ResponseItem('earliestDatestamp', ''),
            ResponseItem('deletedRecord', 'no'),
            ResponseItem('granularity', 'YYYY-MM-DDThh:mm:ssZ'),
            ResponseItem('adminEmail', self.request.repository.admin_email)
        ]

class ResponseItem(object):
    def __init__(self, name, text):
        self.name = name
        self.text = text


oaipmh_attributes = {
    'xmlns': 'http://www.openarchives.org/OAI/2.0/',
    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xsi:schemaLocation': 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
}


