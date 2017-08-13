# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:21:05
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-13 15:42:17

from __future__ import print_function, division, absolute_import
import os

__version__ = "1.11.0dev"  # sciserver release version


class SciServerConfig(object):
    ''' Global configuration for SciServer

        The sciserver.config contains important parameters for the correct functioning of the
        SciServer package.

        Although these parameters must be set/defined by the admin or user before the installation of the
        package, they can also be accessed and changed on-the-fly while on the python session.

        - **config.CasJobsRESTUri**: defines the base URL of the CasJobs web API (string).
          E.g., "https://skyserver.sdss.org/CasJobs/RestApi"

        - **config.AuthenticationURL**: defines the base URL of the Authentication web service API (string).
          E.g., "https://portal.sciserver.org/login-portal/keystone/v3/tokens"

        - **config.SciDriveHost**: defines the base URL of the SciDrive web service API (string).
          E.g., "https://www.scidrive.org"

        - **config.SkyQueryUrl**: defines the base URL of the SkyQuery web service API (string).
          E.g., "http://voservices.net/skyquery/Api/V1"

        - **config.SkyServerWSurl**: defines the base URL of the SkyServer web service API (string).
          E.g., "https://skyserver.sdss.org"

        - **config.DataRelease**: defines the SDSS data release (string), to be used to build the full
          SkyServer API url along with  config.SkyServerWSurl.
          E.g., "DR13"

        - **config.KeystoneTokenPath**: defines the local path (string) to the file containing the
          user's authentication token in the SciServer-Compute environment.
          E.g., "/home/idies/keystone.token". Unlikely to change since it is hardcoded in SciServer-Compute.

        - **config.version**: defines the SciServer release version tag (string), to which this
          package belongs.
          E.g., "1.11.0"

    '''

    def __init__(self):
        ''' Initialize the config '''
        self.set_paths()
        self.version = __version__
        self.token = None

    def set_paths(self):
        ''' Sets the initial paths for SciServer routes '''

        # URLs for accessing SciServer web services (API endpoints)
        self.CasJobsRESTUri = "https://skyserver.sdss.org/CasJobs/RestApi"
        self.AuthenticationURL = "https://portal.sciserver.org/login-portal/keystone/v3/tokens"
        self.SciDriveHost = "https://www.scidrive.org"
        self.SkyQueryUrl = "http://voservices.net/skyquery/Api/V1"
        self.SkyServerWSurl = "https://skyserver.sdss.org"
        self.DataRelease = "DR13"

        # this path to the file containing the user's keystone token is hardcoded in the sciserver-compute environment
        self.KeystoneTokenPath = "/home/idies/keystone.token"

    def isSciServerComputeEnvironment(self):
        """
        Checks whether the library is being run within the SciServer-Compute environment.

        Returns:
            iscompute (bool):
                True if the library is being run within the SciServer-Compute environment, and False if not.
        """
        return os.path.isfile(self.KeystoneTokenPath)

    def get_token(self):
        ''' Get a token from the sciserver environment

        Determines if a user is inside the compute system or not and either
        uses auth.getToken() or auth.login().  Sets the token into the config

        '''
        if not self.token:
            from sciserver import authentication as auth
            if self.isSciServerComputeEnvironment():
                self.token = auth.getToken()
            else:
                self.token = auth.login()


# create the config object
config = SciServerConfig()

