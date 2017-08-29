# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 14:27:25
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-29 11:41:31

from __future__ import print_function, division, absolute_import
from sciserver.authentication import Authentication


class TestAuthentication(object):

    def test_allmethods(self, auth, userdata):

        newToken1 = "myToken1"
        newToken2 = "myToken2"
        username, password = userdata

        token1 = auth.login()
        token2 = auth.getToken()
        token3 = auth.token.value
        user = auth.getKeystoneUserWithToken()

        assert token1 != ""
        assert token1 is not None
        assert token1 == token2
        assert token1 == token3
        assert user.userName == username
        assert user.userid is not None
        assert user.userid != ""

        auth.setToken(newToken1)
        assert newToken1 == auth.getToken()


