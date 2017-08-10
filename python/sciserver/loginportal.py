# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:29:22
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 08:33:23

from __future__ import print_function, division, absolute_import
import warnings
import sciserver.authentication as auth

__author__ = 'mtaghiza, gerard'


class KeystoneUser(object):
    """
    .. warning:: Deprecated. Use auth.KeystoneUser instead.\n

    The class KeystoneUser stores the 'id' and 'name' of the user.
    """
    warnings.warn("Using SciServer.LoginPortal.KeystoneUser is deprecated. "
                  "Use auth.KeystoneUser instead.", DeprecationWarning, stacklevel=2)
    userid = "KeystoneID"
    userName = "User Name"


def getKeystoneUserWithToken(token):
    """
    .. warning:: Deprecated. Use auth.getKeystoneUserWithToken instead.\n

    Returns the name and Keystone id of the user corresponding to the specified token.

    :param token: Sciserver's authentication token (string) for the user.
    :return: Returns a KeystoneUser object, which stores the name and id of the user.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error.
    :example: token = Authentication.getKeystoneUserWithToken(Authentication.getToken())

    .. seealso:: Authentication.getToken, Authentication.login, Authentication.setToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneUserWithToken is deprecated. "
                  "Use auth.getKeystoneUserWithToken instead.", DeprecationWarning, stacklevel=2)
    return auth.getKeystoneUserWithToken(token)


def login(UserName, Password):
    """
    .. warning:: Deprecated. Use auth.login instead.\n

    Logs the user into SciServer and returns the authentication token.
    This function is useful when SciScript-Python library methods are executed outside the
    SciServer-Compute environment.  In this case, the session authentication token does not
    exist (and therefore can't be automatically recognized), so the user has to use Authentication.login
    in order to log into SciServer manually and get the authentication token. authentication.login also
    sets the token value in the python instance argument variable "--ident", and as the local
    object authentication.token (of class Authentication.Token).

    :param UserName: name of the user (string)
    :param Password: password of the user (string)
    :return: authentication token (string)
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error.
    :example: token = Authentication.login('loginName','loginPassword')

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.getToken, Authentication.setToken, Authentication.token.
    """
    warnings.warn("Using SciServer.LoginPortal.login is deprecated."
                  "Use auth.login instead.", DeprecationWarning, stacklevel=2)
    return auth.login(UserName, Password)


def getToken():
    """
    .. warning:: Deprecated. Use auth.getToken instead.\n

    Returns the SciServer authentication token of the user. First, will try to return Authentication.token.value.
    If Authentication.token.value is not set, Authentication.getToken will try to return the token value in the python instance argument variable "--ident".
    If this variable does not exist, will try to return the token stored in Config.KeystoneTokenFilePath. Will return a None value if all previous steps fail.

    :return: authentication token (string)
    :example: token = Authentication.getToken()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token.

    """
    warnings.warn("Using SciServer.LoginPortal.getToken is deprecated. "
                  "Use auth.getToken instead.", DeprecationWarning, stacklevel=2)
    return auth.getToken()


def identArgIdentifier():
    """
    .. warning:: Deprecated. Use auth.identArgIdentifier instead.\n

    Returns the name of the python instance argument variable where the user token is stored.

    :return: name (string) of the python instance argument variable where the user token is stored.
    :example: name = Authentication.identArgIdentifier()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.getToken, Authentication.token.
    """
    warnings.warn("Using auth.identArgIdentifier is deprecated. "
                  "Use auth.identArgIdentifier instead.", DeprecationWarning, stacklevel=2)
    return auth.identArgIdentifier()


def getKeystoneToken():
    """
    .. warning:: Deprecated. Use Authentication.getToken instead.\n

    Returns the users keystone token passed into the python instance with the --ident argument.

    :return: authentication token (string)
    :example: token = Authentication.getKeystoneToken()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token, Authentication.getToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. "
                  "Use auth.getToken instead.", DeprecationWarning, stacklevel=2)
    return auth.getKeystoneToken()


def setKeystoneToken(token):
    """
    .. warning:: Deprecated. Use Authentication.setToken instead.\n

    Sets the token as the --ident argument

    :param _token: authentication token (string)
    :example: Authentication.setKeystoneToken("myToken")

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token, Authentication.getToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. "
                  "Use auth.setToken instead.", DeprecationWarning, stacklevel=2)
    auth.setKeystoneToken(token)

