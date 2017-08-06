# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-06 18:39:54
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-06 19:01:56

from __future__ import print_function, division, absolute_import
import warnings


class SciServerError(Exception):
    pass


class SciServerAPIError(SciServerError):
    def __init__(self, message=None):

        if not message:
            message = 'Error with Http Response from SkyServer API'
        else:
            message = 'Http response error from SkyServer API. {0}'.format(message)

        super(SciServerAPIError, self).__init__(message)


class SciServerDeprecationWarning(DeprecationWarning):
    """A warning for deprecated features."""
    pass
