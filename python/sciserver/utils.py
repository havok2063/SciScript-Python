# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-06 22:12:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 14:50:17

from __future__ import print_function, division, absolute_import
from functools import wraps
from sciserver.exceptions import SciServerError, SciServerAPIError
from sciserver import config
import requests


def checkAuth(func):
    ''' Decorator that checks if a token has been generated

    Function Decorator to check if a token has already been generated.
    If not it raises an error and tells you to log in.  Otherwise it
    returns the function and proceeds as normal.

    Returns:
        The decorated function

    Raises:
        SciServerError: User token is not defined. First log into SciServer.

    Example:
        >>>
        >>> @checkauth
        >>> def my_function():
        >>>     return 'I am working function'
        >>>

    '''

    @wraps(func)
    def wrapper(*args, **kwargs):
        token = config.get_token()
        if not token:
            raise SciServerError('User token is not defined. First log into SciServer.')
        else:
            return func(*args, **kwargs)
    return wrapper


def check_response(response, errmsg='Error'):
    ''' Checks the response

    Checks the response from a given request.  Raises an error
    for any response with a non 200 status_code .

    Parameters:
        response:
            the Python requests response object
        errmsg (str):
            A custom error message for the type of request being sent

    Returns:
        the HTTP response

    Raises:
        SciServerApiError: upon any HTTPError caused by a bad status code

    '''

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
    ''' Makes a request header

    Makes a request header to be passed along.  Includes the Content-Type,
    Accept arguments.  Also includes an X-Auth-Token is a token is present.

    Parameters:
        content_type (str):
            The request header Content-Type (default: application/json)
        accept_header (str):
            The request header Accept (default: application/json)

    Returns:
        headers (dict):
            The dictionary to be used as a request header

    '''

    headers = {'Content-Type': content_type, 'Accept': accept_header}

    # check for auth token
    token = config.get_token()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    return headers


def send_request(url, reqtype='get', data=None, content_type='application/json',
                 acceptHeader='text/plain', errmsg='Error', stream=None):
    ''' Sends a request to the server

    Parameters:
        url (str):
            The url path for the request
        reqtype (str):
            The type of request to perform.  Default is get.  Choices are get, post, put, delete
        data ({str|dict}):
            Optional data to send in the request
        content_type (str):
            the header Content-Type argument.  Default is application/json
        acceptHeader (str):
            the header Accept argument. Default is text/plain
        errmsg (str):
            custom error message in case of faults
        stream (bool):
            optional.  if False, the response content will be immediately downloaded

    Returns:
        response:
            The HTTP response object

    Raises:
        SciServerError: upon any error to occur when sending the requests

    Example:
        >>> # to send a simple get request
        >>> response = send_request(url, errmsg='Error when executing a sql query.')
        >>>
        >>> # to send a post request
        >>> response = send_request(jobsURL, reqtype='post', data=data, content_type='application/json',
        >>>        acceptHeader='application/json', errmsg='Error when submitting job on queue')

    '''

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


class Task(object):
    ''' This class describes a SciServer Task '''

    def __init__(self, name=None, use_base=None, component='SciServer'):
        self.use_base = use_base
        self.component = component
        self.set_name(name)

    @property
    def base_name(self):
        if config.isSciServerComputeEnvironment():
            base = "Compute.SciScript-Python.{0}".format(self.component)
        else:
            base = "SciScript-Python.{0}".format(self.component)
        return base

    def set_name(self, name):
        if name:
            if self.use_base:
                self.name = '{0}.{1}'.format(self.base_name, name)
            else:
                self.name = name
        else:
            self.name = self.base_name

