# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-06 22:12:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 10:07:48

from __future__ import print_function, division, absolute_import
from functools import wraps
from sciserver.exceptions import SciServerError
from sciserver import authentication, config


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
