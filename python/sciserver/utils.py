# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-06 22:12:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 10:23:33

from __future__ import print_function, division, absolute_import
from functools import wraps
from sciserver.exceptions import SciServerError, SciServerAPIError
from sciserver import authentication, config
import requests


def checkAuth(func):
    '''Decorator that checks if a token has been generated'''

    @wraps(func)
    def wrapper(*args, **kwargs):
        token = authentication.getToken() or config.token
        if not token:
            raise SciServerError('User token is not defined. First log into SciServer.')
        else:
            return func(*args, **kwargs)
    return wrapper


def check_response(response, errmsg='Error'):
    ''' Checks the response '''

    try:
        isbad = response.raise_for_status()
    except requests.HTTPError as http:
        err = response.content.decode()
        raise SciServerAPIError('{0}\n {1}: {2}'.format(http, errmsg, err))
    else:
        assert isbad is None, 'Http status code should not be bad'
        assert response.ok is True, 'Ok status should be true'
        return response


def make_header(content_type='application/json', accept_header='text/plain'):
    ''' Make a request header '''

    headers = {'Content-Type': content_type, 'Accept': accept_header}

    # check for auth token
    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    return headers


def send_request(url, reqtype='get', data=None, content_type='application/json',
                 acceptHeader='text/plain', errmsg='Error', stream=None):
    ''' Sends a request to the server '''

    headers = make_header(content_type=content_type, accept_header=acceptHeader)

    # send the request
    try:
        if reqtype == 'get':
            response = requests.get(url, headers=headers, stream=stream)
        elif reqtype == 'post':
            response = requests.post(url, data=data, headers=headers, stream=stream)
        elif reqtype == 'put':
            response = requests.put(url, data=data, headers=headers, stream=stream)
        elif reqtype == 'delete':
            response = requests.delete(url, headers=headers, stream=stream)
    except Exception as e:
        raise SciServerError("A requests error occurred attempting to send: {0}".format(e))
    else:
        resp = check_response(response, errmsg=errmsg)
        return resp
