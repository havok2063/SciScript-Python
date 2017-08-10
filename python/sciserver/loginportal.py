# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:29:22
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 11:34:29

from __future__ import print_function, division, absolute_import
import warnings
import sciserver.authentication as auth

__author__ = 'mtaghiza, gerard'


class KeystoneUser(object):
    """
    The class KeystoneUser stores the 'id' and 'name' of the user.

    .. warning:: Deprecated. Use auth.KeystoneUser instead.

    """
    def __init__(self):
        warnings.warn("Using SciServer.LoginPortal.KeystoneUser is deprecated. "
                      "Use auth.KeystoneUser instead.", DeprecationWarning, stacklevel=2)
        self.userid = "KeystoneID"
        self.userName = "User Name"


def getKeystoneUserWithToken(token):
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
    return auth.getKeystoneUserWithToken(token)


def login(UserName, Password):
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
    return auth.login(UserName, Password)


def getToken():
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
    return auth.getToken()


def identArgIdentifier():
    """ Returns the identity name of the token location

    Returns the name of the python instance argument variable where the user token is stored.

    .. warning:: Deprecated. Use auth.identArgIdentifier instead.

    Returns:
        name (str):
            name of the python instance argument variable where the user token is stored.

    Example:
        >>> name = Authentication.identArgIdentifier()

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.getToken, Authentication.token.

    """
    warnings.warn("Using auth.identArgIdentifier is deprecated. "
                  "Use auth.identArgIdentifier instead.", DeprecationWarning, stacklevel=2)
    return auth.identArgIdentifier()


def getKeystoneToken():
    """ Returns a keystone token

    Returns the users keystone token passed into the python instance with the --ident argument.

    .. warning:: Deprecated. Use Authentication.getToken instead.

    Returns:
        token (str):
            authentication token

    Example:
        >>> token = Authentication.getKeystoneToken()

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token, Authentication.getToken.

    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. "
                  "Use auth.getToken instead.", DeprecationWarning, stacklevel=2)
    return auth.getKeystoneToken()


def setKeystoneToken(token):
    """ Sets the token

    Sets the token as the --ident argument

    .. warning:: Deprecated. Use Authentication.setToken instead.

    Parameters:
        _token (str):
            authentication token

    Example:
        >>> Authentication.setKeystoneToken("myToken")

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken,
        Authentication.token, Authentication.getToken.

    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. "
                  "Use auth.setToken instead.", DeprecationWarning, stacklevel=2)
    auth.setKeystoneToken(token)

