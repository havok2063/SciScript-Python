# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 14:27:25
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 09:47:57

from __future__ import print_function, division, absolute_import
from sciserver import authentication


class TestAuthentication(object):

    def test_allmethods(self, userdata):

        newToken1 = "myToken1"
        newToken2 = "myToken2"
        username, password = userdata

        token1 = authentication.login(username, password)
        token2 = authentication.getToken()
        token3 = authentication.getKeystoneToken()
        token4 = authentication.token.value
        user = authentication.getKeystoneUserWithToken(token1)
        iden = authentication.identArgIdentifier()

        assert iden == "--ident="
        assert token1 != ""
        assert token1 is not None
        assert token1 == token2
        assert token1 == token3
        assert token1 == token4
        assert user.userName == username
        assert user.userid is not None
        assert user.userid != ""

        authentication.setToken(newToken1)
        assert newToken1 == authentication.getToken()
        authentication.setKeystoneToken(newToken2)
        assert newToken2 == authentication.getKeystoneToken()


