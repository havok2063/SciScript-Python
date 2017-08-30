# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:24:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 11:16:13

from __future__ import print_function, division, absolute_import
import pytest
import os
from sciserver import config
from sciserver.authentication import Authentication
from sciserver.loginportal import LoginPortal
from sciserver.casjobs import CasJobs
from sciserver.scidrive import SciDrive
from sciserver.skyserver import SkyServer
from sciserver.skyquery import SkyQuery


userinfo = [('testuser', 'testpass')]


@pytest.fixture(scope='session', params=userinfo)
def userdata(request):
    ''' Fixture to loop over user info '''
    user, password = request.param
    return user, password


@pytest.fixture(scope='session')
def auth():
    auth = Authentication()
    auth.netrcpath = os.path.abspath('data/testnetrc')
    print('netrc', os.path.isfile(auth.netrcpath))
    print('path', auth.netrcpath)
    yield auth
    auth = None


@pytest.fixture(scope='session')
def login(token):
    lp = LoginPortal(token=token)
    yield lp
    lp = None


@pytest.fixture(scope='session')
def cas():
    cas = CasJobs()
    yield cas
    cas = None


@pytest.fixture(scope='session')
def sci():
    sci = SciDrive()
    yield sci
    sci = None


@pytest.fixture(scope='session')
def sky():
    sky = SkyServer()
    yield sky
    sky = None


@pytest.fixture(scope='session')
def skquery():
    skquery = SkyQuery()
    yield skquery
    skquery = None


@pytest.fixture(scope='session')
def token(auth):
    ''' Fixture to generate a token using auth '''
    token = auth.login()
    config.token = token
    yield token
    token = None



