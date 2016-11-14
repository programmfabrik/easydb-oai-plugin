# -*- coding: utf-8 -*-

import context
import oai_modules.repository
import oai_modules.util

def easydb_server_start(easydb_context):
    global repository_base_url
    url_prefix = easydb_context.get_config('system.plugins.url_prefix_internal', False)
    if url_prefix is None:
        url_prefix = easydb_context.get_config('system.plugins.url_prefix')
    repository_base_url = '{}/api/plugin/base/oai/oai'.format(url_prefix)
    easydb_context.register_callback('api', { 'name': 'oai', 'callback': 'oai'})

@oai_modules.util.handle_exceptions
def oai(easydb_context, parameters):
    global repository_base_url
    # method
    if (parameters['method'] not in ['GET', 'POST']):
        return oai_modules.util.http_text_response('Method Not Allowed: only GET or POST requests are allowed\n', 405)
    # base config
    base_config = easydb_context.get_config('base.system')
    if not context.get_json_value(base_config, 'oai_pmh.enabled'):
        return oai_modules.util.http_text_response('OAI/PMH is disabled', 403)
    repository_name = context.get_json_value(base_config, 'oai_pmh.repository_name')
    if len(repository_name) == 0:
        return oai_modules.util.http_text_response('OAI/PMH is disabled (no repository name configured)', 403)
    admin_email = context.get_json_value(base_config, 'oai_pmh.admin_email')
    if len(admin_email) == 0:
        return oai_modules.util.http_text_response('OAI/PMH is disabled (no admin e-mail configured)', 403)
    namespace_identifier = context.get_json_value(base_config, 'oai_pmh.namespace_identifier')
    if len(namespace_identifier) == 0:
        return oai_modules.util.http_text_response('OAI/PMH is disabled (no namespace configured)', 403)
    xslts = context.get_json_value(base_config, 'export.xslts')
    metadata_formats = []
    if xslts is not None:
        for xslt in xslts:
            prefix = context.get_json_value(xslt, 'oai_pmh_prefix')
            if len(prefix) > 0:
                metadata_formats.append(oai_modules.repository.MetadataFormat('xslt', prefix, '', ''))
    # process
    repository = oai_modules.repository.Repository(
        easydb_context,
        repository_base_url,
        repository_name,
        namespace_identifier,
        admin_email,
        metadata_formats)
    qs_parameters = oai_modules.util.parse_query_string(parameters['query_string'])
    response = repository.process_request(qs_parameters)
    return oai_modules.util.http_xml_response(str(response))
