# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 14:27:33
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 14:36:07

from __future__ import print_function, division, absolute_import
from sciserver import loginportal


class TestLoginPortal(object):

    def test_allmethods(self, userdata):
        newToken1 = "myToken1"
        newToken2 = "myToken2"
        username, password = userdata

        token1 = loginportal.login(username, password)
        token2 = loginportal.getToken()
        token3 = loginportal.getKeystoneToken()
        user = loginportal.getKeystoneUserWithToken(token1)
        iden = loginportal.identArgIdentifier()

        assert iden == "--ident="
        assert token1 != ""
        assert token1 is not None
        assert token1 == token2
        assert token1 == token3
        assert user.userName is not None
        assert user.userName != ""
        assert user.id is not None
        assert user.id != ""

        loginportal.setKeystoneToken(newToken1)
        assert newToken1 == loginportal.getKeystoneToken()
