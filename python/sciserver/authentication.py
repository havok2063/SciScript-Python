# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:41:52
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-29 12:17:50

from __future__ import print_function, division, absolute_import
import json
import os.path
import warnings
import requests
import netrc
from sciserver import config
from sciserver.exceptions import SciServerError
from sciserver.utils import send_request

__author__ = 'gerard,mtaghiza'


class KeystoneUser(object):
    """
    The class KeystoneUser stores the 'id' and 'name' of the user.
    """
    def __init__(self, userid=None, userName=None):
        self.userid = userid
        self.userName = userName


class Token(object):
    """
    The class token stores the authentication token of the user in a particular session.
    """
    def __init__(self, value=None):
        self.value = value


class Authentication(object):
    ''' '''

    def __init__(self, token=None):
        self.token = Token(value=token)
        self.tokenFile = config.KeystoneTokenPath
        self.loginURL = config.AuthenticationURL
        self.ident = '--ident='
        self.portal_host = 'portal.sciserver.org'
        self.netrcpath = os.path.join(os.path.expanduser('~'), '.netrc')

    def getKeystoneUserWithToken(self):
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

        assert self.token.value is not None, 'Must have an auth token set'

        loginURL = os.path.join(self.loginURL, self.token.value)

        response = send_request(loginURL, content_type='application/json',
                                errmsg='Error when getting the keystone user with token {0}.'.format(self.token))
        if response.ok:
            jsonres = json.loads(response.content.decode())
            user = jsonres["token"]["user"]
            ksu = KeystoneUser(userid=user['id'], userName=user['name'])

            return ksu

    def _read_netrc(self):
        ''' Read a users netrc file '''

        if os.path.isfile(self.netrcpath):
            netfile = netrc.netrc(self.netrcpath)
            if self.portal_host in netfile.hosts:
                user, acct, passwd = netfile.authenticators(self.portal_host)
                return user, passwd
            else:
                raise SciServerError('{0} not found in your netrc file.  Add it to login to SciServer.'.format(self.portal_host))
        else:
            raise SciServerError('No .netrc file found.  Cannot login to SciServer!')

    def login(self):
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

        user, passwd = self._read_netrc()

        auth = {"auth": {"identity": {"password": {"user": {"name": user, "password": passwd}}}}}
        data = json.dumps(auth).encode()
        headers = {'Content-Type': "application/json"}

        response = requests.post(self.loginURL, data=data, headers=headers)
        if response.ok:
            _token = response.headers['X-Subject-Token']
            self.setToken(_token)
            return _token
        else:
            raise Exception("Error when logging in. Http Response from the SciServer API returned "
                            "status code {0}: \n {1}".format(response.status_code, response.content.decode()))

    def setToken(self, _token):
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
            self.token.value = _token
            config.token = _token

    def getToken(self):
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

        if config.isSciServerComputeEnvironment():
            if os.path.isfile(self.tokenFile):
                with open(self.tokenFile, 'r') as f:
                    _token = f.read().rstrip('\n')
                    if _token is not None and _token != "":
                        self.token.value = _token
                        config.token = _token
                        return _token
                    else:
                        warnings.warn("In Authentication.getToken: Cannot find token in system "
                                      "token file {0}.".format(self.tokenFile), Warning, stacklevel=2)
                        return None
            else:
                warnings.warn("In Authentication.getToken: Cannot find system token "
                              "file {0}.".format(self.tokenFile), Warning, stacklevel=2)
                return None
        else:
            if self.token.value is not None:
                config.token = self.token.value
                return self.token.value
            else:
                _token = None

                if _token is not None and _token != "":
                    self.token.value = _token
                    config.token = _token
                    return _token
                else:
                    warnings.warn("In Authentication.getToken: Authentication token is not defined: "
                                  "the user did not log in with the Authentication.login function, or the token "
                                  "has not been stored in the command line argument --ident.", Warning, stacklevel=2)
                    return None

