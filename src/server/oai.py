# -*- coding: utf-8 -*-

import oai_modules.repository
import oai_modules.util

def easydb_server_start(easydb_context):
    global repository
    url_prefix = easydb_context.get_config('system.plugins.url_prefix_internal', False)
    if url_prefix is None:
        url_prefix = easydb_context.get_config('system.plugins.url_prefix')
    repository_base_url = '{}/api/plugin/base/oai/oai'.format(url_prefix)
    repository = oai_modules.repository.OAIRepository(repository_base_url)
    easydb_context.register_callback('api', { 'name': 'oai', 'callback': 'oai'})

@oai_modules.util.handle_exceptions
def oai(easydb_context, parameters):
    global repository
    if (parameters['method'] not in ['GET', 'POST']):
        return oai_modules.util.http_text_response('Method Not Allowed: only GET or POST requests are allowed\n', 405)
    qs_parameters = oai_modules.util.parse_query_string(parameters['query_string'])
    response = repository.process_request(qs_parameters)
    return oai_modules.util.http_text_response(str(response))
