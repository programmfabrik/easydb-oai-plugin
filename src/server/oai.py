# -*- coding: utf-8 -*-

import context
import oai_modules.repository
import oai_modules.util

def easydb_server_start(easydb_context):
    global repository_base_url
    url_prefix = easydb_context.get_config('system.plugins.url_prefix_internal', False)
    if url_prefix is None:
        url_prefix = easydb_context.get_config('system.plugins.url_prefix')
    while len(url_prefix) > 0 and url_prefix.endswith("/"):
        url_prefix = url_prefix[:-1]
    repository_base_url = '{}/api/plugin/base/oai/oai'.format(url_prefix)
    easydb_context.register_callback('api', { 'name': 'oai', 'callback': 'oai'})

@oai_modules.util.handle_exceptions
def oai(easydb_context, parameters):
    global repository_base_url
    metadata_formats = []
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
    tagfilter_sets_js = context.get_json_value(base_config, 'oai_pmh.tagfilter_sets')
    xslts = context.get_json_value(base_config, 'export.xslts')
    if xslts is not None:
        for xslt in xslts:
            use_for_oai_pmh = context.get_json_value(xslt, 'use_for_oai_pmh')
            if not isinstance(use_for_oai_pmh, bool) or not use_for_oai_pmh:
                continue
            prefix = context.get_json_value(xslt, 'oai_pmh_prefix')
            if len(prefix) < 1:
                continue
            schema = context.get_json_value(xslt, 'schema')
            namespace = context.get_json_value(xslt, 'namespace')
            metadata_formats.append(oai_modules.repository.MetadataFormat(
                'xslt', prefix,
                schema if schema is not None else '',
                namespace if namespace is not None else ''
            ))
    include_eas_urls = context.get_json_value(base_config, 'oai_pmh.include_eas_urls')
    merge_linked_objects = context.get_json_value(base_config, 'oai_pmh.merge_linked_objects')

    # process
    repository = oai_modules.repository.Repository(
        easydb_context,
        repository_base_url,
        repository_name,
        namespace_identifier,
        admin_email,
        metadata_formats,
        tagfilter_sets_js,
        include_eas_urls,
        merge_linked_objects)
    response = repository.process_request(parameters['query_string_parameters'])
    return oai_modules.util.http_xml_response(str(response))
