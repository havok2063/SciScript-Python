# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:36:16
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-04 16:02:22

from __future__ import print_function, division, absolute_import
from sciserver import authentication, config


userNames = ['matlab', 'recount']
userPasswords = ['matlab', 'recount']
userTokens = []

for i in range(len(userNames)):
    authentication.login(userNames[i], userPasswords[i])
    token = authentication.getKeystoneToken()
    userTokens.append(token)

