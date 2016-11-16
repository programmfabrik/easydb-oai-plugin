# -*- coding: utf-8 -*-

import sys
import traceback
import re
import json
import base64
from context import EasydbException, EasydbError

def handle_exceptions(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except InternalError as e:
            return http_text_response(e.message, 500)
        except EasydbException as e:
            return http_text_response('internal error: {}'.format(e), 500)
        except EasydbError as e:
            return http_text_response('internal error: {}'.format(e), 500)
        except BaseException as e:
            exc_info = sys.exc_info()
            stack = traceback.extract_stack()
            tb = traceback.extract_tb(exc_info[2])
            full_tb = stack[:-1] + tb
            exc_line = traceback.format_exception_only(*exc_info[:2])
            traceback_info = '\n'.join([
                'Traceback (most recent call last)',
                ''.join(traceback.format_list(full_tb)),
                ''.join(exc_line)
            ])
            print (traceback_info)
            return http_text_response('internal error: unexpected error occurred', 500)
    return func_wrapper

def http_text_response(text, status_code=200):
    return {
        'status_code': status_code,
        'body': text + '\n',
        'headers': {
            'Content-Type': 'text/plain; charset=utf-8'
        }
    }

def http_xml_response(text):
    return {
        'status_code': 200,
        'body': text,
        'headers': {
            'Content-Type': 'text/xml; charset=utf-8'
        }
    }

class InternalError(Exception):
    def __init__(self, message):
        self.message = message

def tokenize(info_js):
    return base64.b64encode(json.dumps(info_js, separators=(',', ':')))

def untokenize(token):
    return json.loads(base64.b64decode(token))
