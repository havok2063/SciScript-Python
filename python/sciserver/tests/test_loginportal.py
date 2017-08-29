# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 14:27:33
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-29 12:16:27

from __future__ import print_function, division, absolute_import


class TestLoginPortal(object):

    def test_allmethods(self, login, userdata):
        newToken1 = "myToken1"
        newToken2 = "myToken2"
        username, password = userdata

        token1 = login.login()
        token2 = login.getToken()
        user = login.getKeystoneUserWithToken()

        assert token1 != ""
        assert token1 is not None
        assert token1 == token2
        assert user.userName is not None
        assert user.userName != ""
        assert user.userid is not None
        assert user.userid != ""
