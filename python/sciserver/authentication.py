# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:41:52
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 11:23:37

from __future__ import print_function, division, absolute_import
import json
import sys
import os.path
import warnings
import requests
from sciserver import config

__author__ = 'gerard,mtaghiza'


class KeystoneUser(object):
    """
    The class KeystoneUser stores the 'id' and 'name' of the user.
    """
    def __init__(self):
        self.userid = None
        self.userName = None


class Token(object):
    """
    The class token stores the authentication token of the user in a particular session.
    """
    def __init__(self):
        self.value = None


token = Token()


def getKeystoneUserWithToken(token):
    """ Returns Keystone user info after login

    Returns the name and Keystone id of the user corresponding to the specified token.

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
    loginURL = config.AuthenticationURL
    if ~loginURL.endswith("/"):
        loginURL = loginURL + "/"
    loginURL = loginURL + token

    getResponse = requests.get(loginURL)
    if getResponse.status_code != 200:
        raise Exception("Error when getting the keystone user with token {0}."
                        "Http Response from the Authentication API returned status code {1}:"
                        "\n {2}".format(token, getResponse.status_code, getResponse.content.decode()))

    responseJson = json.loads((getResponse.content.decode()))

    ksu = KeystoneUser()
    ksu.userName = responseJson["token"]["user"]["name"]
    ksu.userid = responseJson["token"]["user"]["id"]

    return ksu


def login(UserName, Password):
    """ Logs in a user into SciServer

    Logs the user into SciServer and returns the authentication token.
    This function is useful when SciScript-Python library methods are executed outside
    the SciServer-Compute environment. In this case, the session authentication token
    does not exist (and therefore can't be automatically recognized), so the user has to use
    Authentication.login in order to log into SciServer manually and get the authentication token.
    Authentication.login also sets the token value in the python instance argument variable
    "--ident", and as the local object Authentication.token (of class Authentication.Token).

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
    loginURL = config.AuthenticationURL

    authJson = {"auth": {"identity": {"password": {"user": {"name": UserName, "password": Password}}}}}

    data = json.dumps(authJson).encode()

    headers = {'Content-Type': "application/json"}

    postResponse = requests.post(loginURL, data=data, headers=headers)
    if postResponse.status_code != 200:
        raise Exception("Error when logging in. Http Response from the Authentication API returned "
                        "status code {0}: \n {1}".format(postResponse.status_code, postResponse.content.decode()))

    _token = postResponse.headers['X-Subject-Token']
    setToken(_token)
    return _token


def getToken():
    """ Returns the authentication token of a user

    Returns the SciServer authentication token of the user. First, will try to
    return Authentication.token.value. If Authentication.token.value is not set,
    Authentication.getToken will try to return the token value in the python instance
    argument variable "--ident". If this variable does not exist, will try to return the token
    stored in config.KeystoneTokenFilePath. Will return a None value if all previous steps fail.

    Returns:
        token (str):
            The authentication token

    Example:
        >>> token = Authentication.getToken()

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token.

    """
    try:

        if config.isSciServerComputeEnvironment():
            tokenFile = config.KeystoneTokenPath  # '/home/idies/keystone.token'
            if os.path.isfile(tokenFile):
                with open(tokenFile, 'r') as f:
                    _token = f.read().rstrip('\n')
                    if _token is not None and _token != "":
                        token.value = _token

                        found = False
                        ident = identArgIdentifier()
                        for arg in sys.argv:
                            if (arg.startswith(ident)):
                                sys.argv.remove(arg)
                                sys.argv.append(ident + _token)
                                found = True
                        if not found:
                            sys.argv.append(ident + _token)

                        return _token
                    else:
                        warnings.warn("In Authentication.getToken: Cannot find token in system "
                                      "token file {0}.".format(config.KeystoneTokenPath), Warning, stacklevel=2)
                        return None
            else:
                warnings.warn("In Authentication.getToken: Cannot find system token "
                              "file {0}.".format(config.KeystoneTokenPath), Warning, stacklevel=2)
                return None
        else:
            if token.value is not None:
                return token.value
            else:
                _token = None
                ident = identArgIdentifier()
                for arg in sys.argv:
                    if (arg.startswith(ident)):
                        _token = arg[len(ident):]

                if _token is not None and _token != "":
                    token.value = _token
                    return _token
                else:
                    warnings.warn("In Authentication.getToken: Authentication token is not defined: "
                                  "the user did not log in with the Authentication.login function, or the token "
                                  "has not been stored in the command line argument --ident.", Warning, stacklevel=2)
                    return None

    except Exception as e:
        raise e


def setToken(_token):
    """ Sets an authentication token

    Sets the SciServer authentication token of the user in the variable Authentication.token.value,
    as well as in the python instance argument variable "--ident".

    Parameters:
        _token (str):
            Sciserver's authentication token for the user

    Example:
        >>> Authentication.setToken('myToken')

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.getToken, Authentication.token.

    """
    if _token is None:
        warnings.warn("Authentication token is being set with a None value.", Warning, stacklevel=2)
    if _token == "":
        warnings.warn("Authentication token is being set as an empty string.", Warning, stacklevel=2)

    if config.isSciServerComputeEnvironment():
        warnings.warn("Authentication token cannot be set to arbitary value when inside "
                      "SciServer-Compute environment.", Warning, stacklevel=2)
    else:
        token.value = _token

        found = False
        ident = identArgIdentifier()
        for arg in sys.argv:
            if (arg.startswith(ident)):
                sys.argv.remove(arg)
                sys.argv.append(ident + _token)
                found = True
        if not found:
            sys.argv.append(ident + _token)


def identArgIdentifier():
    """ Returns the identity name of the token location

    Returns the name of the python instance argument variable where the user token is stored.

    Returns:
        name (str):
            name of the python instance argument variable where the user token is stored.

    Example:
        >>> name = Authentication.identArgIdentifier()

    See Also:
        Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.getToken, Authentication.token.

    """
    return "--ident="


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
    warnings.warn("Using SciServer.Authentication.getKeystoneToken is deprecated."
                  "Use SciServer.Authentication.getToken instead.", DeprecationWarning, stacklevel=2)

    _token = None
    ident = identArgIdentifier()
    for arg in sys.argv:
        if (arg.startswith(ident)):
            _token = arg[len(ident):]

    # if (_token == ""):
    #     raise EnvironmentError("Keystone token is not in the command line argument --ident.")
    if _token is None or _token == "":
        warnings.warn("Keystone token is not defined, and is not stored in the command line argument --ident.", Warning, stacklevel=2)

    return _token


def setKeystoneToken(_token):
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
    warnings.warn("Using SciServer.Authentication.setKeystoneToken is deprecated."
                  "Use SciServer.Authentication.setToken instead.", DeprecationWarning, stacklevel=2)

    if _token is None:
        warnings.warn("Authentication token is being set with a None value.", Warning, stacklevel=2)
    if _token == "":
        warnings.warn("Authentication token is being set as an empty string.", Warning, stacklevel=2)

    found = False
    ident = identArgIdentifier()
    for arg in sys.argv:
        if (arg.startswith(ident)):
            sys.argv.remove(arg)
            sys.argv.append(ident + str(_token))
            found = True
    if not found:
        sys.argv.append(ident + str(_token))
