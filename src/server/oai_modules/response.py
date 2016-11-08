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
    def __init__(self, request, earliest_datestamp):
        super(Identify, self).__init__(request)
        self.earliest_datestamp = earliest_datestamp
    def get_response_items(self):
        return [
            ResponseItem('repositoryName', self.request.repository.name),
            ResponseItem('baseURL', self.request.repository.base_url),
            ResponseItem('protocolVersion', '2.0'),
            ResponseItem('earliestDatestamp', self.earliest_datestamp),
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
    def __init__(self, request, formats):
        super(ListMetadataFormats, self).__init__(request)
        self.formats = formats
    def get_response_items(self):
        return [self.format_to_response_item(f) for f in self.formats]
    def format_to_response_item(self, f):
        metadataFormat = ResponseItem('metadataFormat')
        metadataFormat.subitems = [
            ResponseItem('metadataPrefix', f.prefix),
            ResponseItem('schema', f.schema),
            ResponseItem('metadataNamespace', f.namespace)
        ]
        return metadataFormat

class Records(Response):
    def __init__(self, request, records, only_header=False):
        super(Records, self).__init__(request)
        self.records = records
        self.only_header = only_header
    def get_response_items(self):
        return [self.record_to_response_item(record) for record in self.records]
    def record_to_response_item(self, record):
        header = ResponseItem('header')
        header.subitems = [
            ResponseItem('identifier', record.identifier),
            # FIXME
            ResponseItem('datestamp', '2016-01-01T00:00:00Z')
        ]
        header.subitems += [ResponseItem('setSpec', set_spec) for set_spec in record.set_specs]
        if self.only_header:
            return header
        metadata = ResponseItem('metadata')
        # FIXME
        foo = ResponseItem('foo', 'bar')
        metadata.subitems = [foo]
        record_item = ResponseItem('record')
        record_item.subitems = [header, metadata]
        return record_item

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


