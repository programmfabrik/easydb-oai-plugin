# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET
import xml.dom.minidom
import datetime


class Response(object):
    def __init__(self, request):
        self.request = request
        self.error_code = None

    def __str__(self):
        schemaLocations = [
            'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd']
        for namespace in self.get_response_namespaces():
            schema_def = namespace.url + ' ' + namespace.schema
            if schema_def not in schemaLocations:
                schemaLocations.append(schema_def)
            ET.register_namespace(namespace.prefix, namespace.url)

        oaipmh_attributes['xsi:schemaLocation'] = ' '.join(schemaLocations)
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
            request.attrib = {}
            for name, values in self.request.parameters.items():
                if len(values) > 0:
                    request.attrib[name] = values[0]
            verb = ET.SubElement(oaipmh, self.request.verb)
            for item in self.get_response_items():
                item.add_sub_element(verb)

        # http://effbot.org/zone/element-namespaces.htm
        # http://effbot.org/zone/copyright.htm
        # > Unless otherwise noted, source code can be be used freely.
        # > Examples, test scripts and other short code fragments can be
        # > considered as being in the public domain.
        def fixup_element_prefixes(elem, uri_map, memo):
            def fixup(name):
                try:
                    return memo[name]

                except KeyError:
                    if name[0] != "{":
                        return
                    uri, tag = name[1:].split("}")
                    if uri in uri_map:
                        new_name = uri_map[uri] + ":" + tag
                        memo[name] = new_name

                        return new_name

            # fix element name
            name = fixup(elem.tag)
            if name:
                elem.tag = name
            # fix attribute names
            for key, value in elem.items():
                name = fixup(key)
                if name:
                    elem.set(name, value)
                    del elem.attrib[key]

        def fixup_xmlns(elem, maps=None):
            if maps is None:
                maps = [{}]

            # check for local overrides
            xmlns = {}
            for key, value in elem.items():
                if key[:6] == "xmlns:":
                    xmlns[value] = key[6:]
            if xmlns:
                uri_map = maps[-1].copy()
                uri_map.update(xmlns)
            else:
                uri_map = maps[-1]

            # fixup this element
            fixup_element_prefixes(elem, uri_map, {})

            # process elements
            maps.append(uri_map)
            for elem in elem:
                fixup_xmlns(elem, maps)
            maps.pop()

        fixup_xmlns(oaipmh)

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
            ResponseItem('deletedRecord', 'no'),
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
            items.append(ResponseItem(
                'resumptionToken', self.resumption_token))

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
        items = [self.record_to_response_item(
            record) for record in self.records]
        if self.resumption_token is not None:
            items.append(ResponseItem(
                'resumptionToken', self.resumption_token))

        return items

    def get_response_namespaces(self):
        ns = []
        if len(self.metadata_format.namespace) > 0:
            ns.append(ResponseNamespace(self.metadata_format.prefix,
                                        self.metadata_format.namespace, self.metadata_format.schema))

        return ns

    def record_to_response_item(self, record):
        header = ResponseItem('header')
        if record.deleted:
            header.attrib['status'] = 'deleted'
        header.subitems = [
            ResponseItem('identifier', record.identifier),
            ResponseItem('datestamp', record.last_modified)
        ]
        header.subitems += [ResponseItem('setSpec', set_spec)
                            for set_spec in record.set_specs]

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
    def __init__(self, prefix, url, schema):
        self.prefix = prefix
        self.url = url
        self.schema = schema


oaipmh_attributes = {
    'xmlns': 'http://www.openarchives.org/OAI/2.0/',
    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
}
