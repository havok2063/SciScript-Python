# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:24:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-09 13:39:45

from __future__ import print_function, division, absolute_import
import pytest
from sciserver import authentication, config


userinfo = [('testuser', 'testpass')]


@pytest.fixture(scope='session', params=userinfo)
def userdata(request):
    ''' Fixture to loop over user info '''
    user, password = request.param
    return user, password


@pytest.fixture(scope='session')
def token(userdata):
    ''' Fixture to generate a token using auth '''
    login, password = userdata
    token = authentication.login(login, password)
    config.token = token
    yield token
    token = None
