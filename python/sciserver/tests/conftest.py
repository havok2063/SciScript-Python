# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:24:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-06 20:24:47

from __future__ import print_function, division, absolute_import
import pytest
from sciserver import authentication


@pytest.fixture(scope='session')
def token():
    login = '***'
    password = '***'
    token = authentication.login(login, password)
    yield token
    token = None
