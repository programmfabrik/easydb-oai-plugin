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
        for namespace in self.get_response_namespaces():
            ET.register_namespace(namespace.prefix, namespace.url)
        responseDate = ET.SubElement(oaipmh, 'responseDate')
        responseDate.text = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        request = ET.SubElement(oaipmh, 'request')
        request.text = self.request.repository.base_url
        if self.error_code is not None:
            error = ET.SubElement(oaipmh, 'error', {'code': self.error_code})
            if self.error_message is not None:
                error.text = self.error_message
        else:
            request.attrib = {}
            for name, values in self.request.parameters.items():
                if len(values) > 0:
                    request.attrib[name] = values[0]
            verb = ET.SubElement(oaipmh, self.request.verb)
            for item in self.get_response_items():
                item.add_sub_element(verb)
        return xml.dom.minidom.parseString(ET.tostring(oaipmh, 'UTF-8')).toprettyxml(indent='\t', encoding='UTF-8')
    def get_response_items(self):
        return []
    def get_response_namespaces(self):
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
            ResponseItem('deletedRecord', 'persistent'),
            ResponseItem('granularity', 'YYYY-MM-DDThh:mm:ssZ'),
            ResponseItem('adminEmail', self.request.repository.admin_email)
        ]

class ListMetadataFormats(Response):
    def __init__(self, request, formats):
        super(ListMetadataFormats, self).__init__(request)
        self.formats = formats
    def get_response_items(self):
        return [self._format_to_response_item(f) for f in self.formats]
    def _format_to_response_item(self, f):
        metadataFormat = ResponseItem('metadataFormat')
        metadataFormat.subitems = [
            ResponseItem('metadataPrefix', f.prefix),
            ResponseItem('schema', f.schema),
            ResponseItem('metadataNamespace', f.namespace)
        ]
        return metadataFormat

class ListSets(Response):
    def __init__(self, request, sets, resumption_token):
        super(ListSets, self).__init__(request)
        self.sets = sets
        self.resumption_token = resumption_token
    def get_response_items(self):
        items = [self._set_to_response_item(s) for s in self.sets]
        if self.resumption_token is not None:
            items.append(ResponseItem('resumptionToken', self.resumption_token))
        return items
    def _set_to_response_item(self, s):
        item = ResponseItem('set')
        item.subitems = [
            ResponseItem('setName', s.name),
            ResponseItem('setSpec', s.spec)
        ]
        return item

class Records(Response):
    def __init__(self, request, records, metadata_format, resumption_token=None):
        super(Records, self).__init__(request)
        self.records = records
        self.metadata_format = metadata_format
        self.resumption_token = resumption_token
    def get_response_items(self):
        items = [self.record_to_response_item(record) for record in self.records]
        if self.resumption_token is not None:
            items.append(ResponseItem('resumptionToken', self.resumption_token))
        return items
    def get_response_namespaces(self):
        ns = []
        if len(self.metadata_format.namespace) > 0:
            ns.append(ResponseNamespace(self.metadata_format.prefix, self.metadata_format.namespace))
        return ns
    def record_to_response_item(self, record):
        header = ResponseItem('header')
        if record.deleted:
            header.attrib['status'] = 'deleted'
        header.subitems = [
            ResponseItem('identifier', record.identifier),
            ResponseItem('datestamp', record.last_modified)
        ]
        header.subitems += [ResponseItem('setSpec', set_spec) for set_spec in record.set_specs]
        if record.metadata is None:
            return header
        metadata = ResponseItem('metadata')
        metadata.subelements = [
            record.metadata
        ]
        record_item = ResponseItem('record')
        record_item.subitems = [header, metadata]
        return record_item

class ResponseItem(object):
    def __init__(self, name, text=None):
        self.name = name
        self.text = text
        self.subitems = []
        self.subelements = []
        self.attrib = {}
    def add_sub_element(self, element):
        se = ET.SubElement(element, self.name)
        se.attrib = self.attrib
        if self.text is not None:
            se.text = self.text
        for subitem in self.subitems:
            subitem.add_sub_element(se)
        for subelement in self.subelements:
            se.append(subelement)

class ResponseNamespace(object):
    def __init__(self, prefix, url):
        self.prefix = prefix
        self.url = url

oaipmh_attributes = {
    'xmlns': 'http://www.openarchives.org/OAI/2.0/',
    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xsi:schemaLocation': 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
}


