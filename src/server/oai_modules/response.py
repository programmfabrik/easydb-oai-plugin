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
                item.add_sub_element(verb)
        return xml.dom.minidom.parseString(ET.tostring(oaipmh, 'UTF-8')).toprettyxml(indent='\t', encoding='UTF-8')
    def get_response_items(self):
        return []

class Error(Response):
    def __init__(self, request, error_code, error_message=None):
        super(Error, self).__init__(request)
        self.error_code = error_code
        self.error_message = error_message

class Identify(Response):
    def __init__(self, request):
        super(Identify, self).__init__(request)
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

class ListSets(Response):
    def __init__(self, request, sets):
        super(ListSets, self).__init__(request)
        self.sets = sets
    def get_response_items(self):
        items = []
        for s in self.sets:
            item = ResponseItem('set')
            item.subitems = [
                ResponseItem('setName', s.name),
                ResponseItem('setSpec', s.spec)
            ]
            items.append(item)
        return items

class ListMetadataFormats(Response):
    def __init__(self, request):
        super(ListMetadataFormats, self).__init__(request)
    def get_response_items(self):
        metadataFormat = ResponseItem('metadataFormat')
        metadataFormat.subitems = [
            ResponseItem('metadataPrefix', 'oai_dc'),
            ResponseItem('schema', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd'),
            ResponseItem('metadataNamespace', 'http://www.openarchives.org/OAI/2.0/oai_dc/')
        ]
        return [metadataFormat]

class GetRecord(Response):
    def __init__(self, request, record):
        super(GetRecord, self).__init__(request)
        self.record = record
    def get_response_items(self):
        header = ResponseItem('header')
        header.subitems = [
            ResponseItem('identifier', self.record.identifier)
        ]
        header.subitems += [ResponseItem('setSpec', set_spec) for set_spec in self.record.set_specs]
        return [header]

class ResponseItem(object):
    def __init__(self, name, text=None):
        self.name = name
        self.text = text
        self.subitems = []
    def add_sub_element(self, element):
        se = ET.SubElement(element, self.name)
        if self.text is not None:
            se.text = self.text
        for subitem in self.subitems:
            subitem.add_sub_element(se)

oaipmh_attributes = {
    'xmlns': 'http://www.openarchives.org/OAI/2.0/',
    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xsi:schemaLocation': 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
}


