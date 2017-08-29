# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:29:22
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-29 12:18:00

from __future__ import print_function, division, absolute_import
import warnings
from sciserver.authentication import Authentication

__author__ = 'mtaghiza, gerard'


class KeystoneUser(object):
    """
    The class KeystoneUser stores the 'id' and 'name' of the user.

    .. warning:: Deprecated. Use auth.KeystoneUser instead.

    """
    def __init__(self, userid='KeystoneID', userName='UserName'):
        warnings.warn("Using SciServer.LoginPortal.KeystoneUser is deprecated. "
                      "Use auth.KeystoneUser instead.", DeprecationWarning, stacklevel=2)
        self.userid = userid
        self.userName = userName


class LoginPortal(object):

    def __init__(self, token=None):
        warnings.warn('SciServer.LoginPortal is deprecated. '
                      'Use Authentication instead', DeprecationWarning, stacklevel=2)
        self.token = token
        self.auth = Authentication(token=self.token)

    def getKeystoneUserWithToken(self):
        """ Returns the keystone user info

        Returns the name and Keystone id of the user corresponding to the specified token.

        .. warning:: Deprecated. Use auth.getKeystoneUserWithToken instead.


        Parameters:
            token (str):
                Sciserver's authentication token for the user.

        Returns:
            an instance of the KeystoneUser object, which stores the name and id of the user.

        Raises:
            Exception: Throws an exception if the HTTP request to the Authentication URL returns an error.

        Example:
            >>> token = Authentication.getKeystoneUserWithToken(Authentication.getToken())

        See Also:
            Authentication.getToken, Authentication.login, Authentication.setToken.

        """
        warnings.warn("Using SciServer.LoginPortal.getKeystoneUserWithToken is deprecated. "
                      "Use auth.getKeystoneUserWithToken instead.", DeprecationWarning, stacklevel=2)
        return self.auth.getKeystoneUserWithToken()

    def login(self):
        """ Logs in a user into SciServer

        Logs the user into SciServer and returns the authentication token.
        This function is useful when SciScript-Python library methods are executed outside
        the SciServer-Compute environment. In this case, the session authentication token
        does not exist (and therefore can't be automatically recognized), so the user has to use
        Authentication.login in order to log into SciServer manually and get the authentication token.
        Authentication.login also sets the token value in the python instance argument variable
        "--ident", and as the local object Authentication.token (of class Authentication.Token).

        .. warning:: Deprecated. Use auth.login instead.

        Parameters:
            UserName (str):
                name of the user
            Password (str):
                password of the user

        Returns:
            token (str):
                authentication token

        Raises:
            Exception: Throws an exception if the HTTP request to the Authentication URL returns an error.

        Example:
            >>> token = Authentication.login('loginName','loginPassword')

        See Also:
            Authentication.getKeystoneUserWithToken, Authentication.getToken, Authentication.setToken, Authentication.token.

        """
        warnings.warn("Using SciServer.LoginPortal.login is deprecated."
                      "Use auth.login instead.", DeprecationWarning, stacklevel=2)
        return self.auth.login()

    def getToken(self):
        """ Returns the authentication token of a user

        Returns the SciServer authentication token of the user. First, will try to
        return Authentication.token.value. If Authentication.token.value is not set,
        Authentication.getToken will try to return the token value in the python instance
        argument variable "--ident". If this variable does not exist, will try to return the token
        stored in config.KeystoneTokenFilePath. Will return a None value if all previous steps fail.

        .. warning:: Deprecated. Use auth.getToken instead.

        Returns:
            token (str):
                The authentication token

        Example:
            >>> token = Authentication.getToken()

        See Also:
            Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token.


        """
        warnings.warn("Using SciServer.LoginPortal.getToken is deprecated. "
                      "Use auth.getToken instead.", DeprecationWarning, stacklevel=2)
        return self.auth.getToken()



