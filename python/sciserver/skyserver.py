# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 16:25:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 10:24:41

from __future__ import print_function, division, absolute_import
from io import StringIO, BytesIO
import pandas
import skimage.io
from sciserver import config
from sciserver.utils import send_request


def get_url(base, data_release=None):
    ''' Create the request URL route

    Appends a named base service to the core url route.

    Parameters:
        base (str):
            The name of the service being requested.
        data_release (str):
            The specific data release to request, if any

    Returns:
        url (str):
            The base API route to use
    '''

    # set the data release
    release = data_release if data_release else config.DataRelease if config.DataRelease else None

    if release:
        url = '{0}/{1}/{2}'.format(config.SkyServerWSurl, release, base)
    else:
        url = '{0}/{2}'.format(config.SkyServerWSurl, base)

    return url


def pad_url(url, **kwargs):
    ''' Pads the url with keyword parameters

    Parameters:
        url (str):
            The base url to append content to
        kwargs (dict):
            All the passed in request parameters to append

    Returns:
        url (str):
            The url with the appended request parameters
    '''

    # check for a taskname and pop it out
    taskname = kwargs.pop('taskname', None)

    # handle apogee/apstar separately
    apogeeid = kwargs.pop('apogee_id', None)
    apstarid = kwargs.pop('apstar_id', None)
    apokey = 'apogee_id' if apogeeid else 'apstar_id' if apstarid else None
    apoid = apogeeid if apogeeid else apstarid if apstarid else None
    if apoid:
        url = '{0}{1}={2}&'.format(url, apokey, apoid)

    # Append the keyword arguments to the url url
    for key, val in kwargs.items():
        if val:
            url = '{0}{1}={2}&'.format(url, key, val)

    # Append the task name
    if taskname:
        if config.isSciServerComputeEnvironment():
            url = "{0}TaskName=Compute.SciScript-Python.SkyServer.{1}&".format(url, taskname)
        else:
            url = "{0}TaskName=SciScript-Python.SkyServer.{1}&".format(url, taskname)

    return url


def sqlSearch(sql, dataRelease=None):
    """
    Executes a SQL query to the SDSS database, and retrieves the result table as a dataframe. Maximum number of rows retrieved is set currently to 500,000.

    :param sql: a string containing the sql query
    :param dataRelease: SDSS data release (string). E.g, 'DR13'. Default value already set in SciServer.config.DataRelease
    :return: Returns the results table as a Pandas data frame.
    :raises: Throws an exception if the HTTP request to the SkyServer API returns an error.
    :example: df = SkyServer.sqlSearch(sql="select 1")

    .. seealso:: CasJobs.executeQuery, CasJobs.submitJob.
    """

    url = get_url('SkyServerWS/SearchTools/SqlSearch?', data_release=dataRelease)

    url = pad_url(url, format='csv', cmd=sql, taskname='sqlSearch')

    response = send_request(url, errmsg='Error when executing a sql query.', stream=True)
    r = response.content.decode()
    return pandas.read_csv(StringIO(r), comment='#', index_col=None)


def getJpegImgCutout(ra, dec, scale=0.7, width=512, height=512, opt="", query="", dataRelease=None):
    """
    Gets a rectangular image cutout from a region of the sky in SDSS, centered at (ra,dec). Return type is numpy.ndarray.\n

    :param ra: Right Ascension of the image's center.
    :param dec: Declination of the image's center.
    :param scale: scale of the image, measured in [arcsec/pix]
    :param width: Right Ascension of the image's center.
    :param ra: Right Ascension of the image's center.
    :param height: Height of the image, measured in [pix].
    :param opt: Optional drawing options, expressed as concatenation of letters (string). The letters options are \n
    \t"G": Grid. Draw a N-S E-W grid through the center\n
    \t"L": Label. Draw the name, scale, ra, and dec on image.\n
    \t"P PhotoObj. Draw a small cicle around each primary photoObj.\n
    \t"S: SpecObj. Draw a small square around each specObj.\n
    \t"O": Outline. Draw the outline of each photoObj.\n
    \t"B": Bounding Box. Draw the bounding box of each photoObj.\n
    \t"F": Fields. Draw the outline of each field.\n
    \t"M": Masks. Draw the outline of each mask considered to be important.\n
    \t"Q": Plates. Draw the outline of each plate.\n
    \t"I": Invert. Invert the image (B on W).\n
    \t(see http://skyserver.sdss.org/public/en/tools/chart/chartinfo.aspx)\n
    :param query: Optional string. Marks with inverted triangles on the image the position of user defined objects. The (RA,Dec) coordinates of these object can be given by three means:\n
    \t1) query is a SQL command of format "SELECT Id, RA, Dec, FROM Table".
    \t2) query is list of objects. A header with RA and DEC columns must be included. Columns must be separated by tabs, spaces, commas or semicolons. The list may contain as many columns as wished.
    \t3) query is a string following the pattern: ObjType Band (low_mag, high_mag).
    \t\tObjType: S | G | P marks Stars, Galaxies or PhotoPrimary objects.\n
    \t\tBand: U | G | R | I | Z | A restricts marks to objects with Band BETWEEN low_mag AND high_mag Band 'A' will mark all objects within the specified magnitude range in any band (ORs composition).\n
    \tExamples:\n
    \t\tS\n
    \t\tS R (0.0, 23.5)\n
    \t\tG A (20, 30)\n
    \t\t(see http://skyserver.sdss.org/public/en/tools/chart/chartinfo.aspx)\n
    :param dataRelease: SDSS data release string. Example: dataRelease='DR13'. Default value already set in SciServer.config.DataRelease
    :return: Returns the image as a numpy.ndarray object.
    :raises: Throws an exception if the HTTP request to the SkyServer API returns an error.
    :example: img = SkyServer.getJpegImgCutout(ra=197.614455642896, dec=18.438168853724, width=512, height=512, scale=0.4, opt="OG", query="SELECT TOP 100 p.objID, p.ra, p.dec, p.r FROM fGetObjFromRectEq(197.6,18.4,197.7,18.5) n, PhotoPrimary p WHERE n.objID=p.objID")
    """

    url = get_url('SkyServerWS/ImgCutout/getjpeg?', data_release=dataRelease)

    url = pad_url(url, ra=ra, dec=dec, scale=scale, width=width, height=height,
                  opt=opt, query=query, taskname='getJpegImgCutout')

    response = send_request(url, errmsg='Error when getting an image cutout.', stream=True)
    return skimage.io.imread(BytesIO(response.content))


def radialSearch(ra, dec, radius=1, coordType="equatorial", whichPhotometry="optical", limit="10", dataRelease=None):
    """
    Runs a query in the SDSS database that searches for all objects within a certain radius from a point in the sky, and retrieves the result table as a Panda's dataframe.\n

    :param ra: Right Ascension of the image's center.\n
    :param dec: Declination of the image's center.\n
    :param radius: Search radius around the (ra,dec) coordinate in the sky. Measured in arcminutes.\n
    :param coordType: Type of celestial coordinate system. Can be set to "equatorial" or "galactic".\n
    :param whichPhotometry: Type of retrieved data. Can be set to "optical" or "infrared".\n
    :param limit: Maximum number of rows in the result table (string). If set to "0", then the function will return all rows.\n
    :param dataRelease: SDSS data release string. Example: dataRelease='DR13'. Default value already set in SciServer.config.DataRelease
    :return: Returns the results table as a Pandas data frame.
    :raises: Throws an exception if the HTTP request to the SkyServer API returns an error.
    :example: df = SkyServer.radialSearch(ra=258.25, dec=64.05, radius=3)

    .. seealso:: SkyServer.sqlSearch, SkyServer.rectangularSearch.
    """

    url = get_url('SkyServerWS/SearchTools/RadialSearch?', data_release=dataRelease)

    url = pad_url(url, format='csv', ra=ra, dec=dec, radius=radius, coordType=coordType,
                  whichPhotometry=whichPhotometry, limit=limit, taskname='radialSearch')

    response = send_request(url, errmsg='Error when executing a radial search.', stream=True)
    r = response.content.decode()
    return pandas.read_csv(StringIO(r), comment='#', index_col=None)


def rectangularSearch(min_ra, max_ra, min_dec, max_dec, coordType="equatorial", whichPhotometry="optical",
                      limit="10", dataRelease=None):
    """
    Runs a query in the SDSS database that searches for all objects within a certain rectangular box defined on the the sky, and retrieves the result table as a Panda's dataframe.\n

    :param min_ra: Minimum value of Right Ascension coordinate that defines the box boundaries on the sky.\n
    :param max_ra: Maximum value of Right Ascension coordinate that defines the box boundaries on the sky.\n
    :param min_dec: Minimum value of Declination coordinate that defines the box boundaries on the sky.\n
    :param max_dec: Maximum value of Declination coordinate that defines the box boundaries on the sky.\n
    :param coordType: Type of celestial coordinate system. Can be set to "equatorial" or "galactic".\n
    :param whichPhotometry: Type of retrieved data. Can be set to "optical" or "infrared".\n
    :param limit: Maximum number of rows in the result table (string). If set to "0", then the function will return all rows.\n
    :param dataRelease: SDSS data release string. Example: dataRelease='DR13'. Default value already set in SciServer.config.DataRelease
    :return: Returns the results table as a Pandas data frame.
    :raises: Throws an exception if the HTTP request to the SkyServer API returns an error.
    :example: df = SkyServer.rectangularSearch(min_ra=258.2, max_ra=258.3, min_dec=64,max_dec=64.1)

    .. seealso:: SkyServer.sqlSearch, SkyServer.radialSearch.
    """

    url = get_url('SkyServerWS/SearchTools/RectangularSearch?', data_release=dataRelease)

    url = pad_url(url, format='csv', min_ra=min_ra, max_ra=max_ra, min_dec=min_dec, coordType=coordType,
                  max_dec=max_dec, whichPhotometry=whichPhotometry, limit=limit, taskname='rectangularSearch')

    response = send_request(url, errmsg='Error when executing a rectangular search.', stream=True)
    r = response.content.decode()
    return pandas.read_csv(StringIO(r), comment='#', index_col=None)


def objectSearch(objId=None, specObjId=None, apogee_id=None, apstar_id=None, ra=None, dec=None,
                 plate=None, mjd=None, fiber=None, run=None, rerun=None, camcol=None, field=None,
                 obj=None, dataRelease=None):
    """
    Gets the properties of the the object that is being searched for. Search parameters:\n

    :param objId: SDSS ObjId.\n
    :param specObjId: SDSS SpecObjId.\n
    :param apogee_id: ID idetifying Apogee target object.\n
    :param apstar_id: unique ID for combined apogee star spectrum.\n
    :param ra: right ascention.\n
    :param dec: declination.\n
    :param plate: SDSS plate number.\n
    :param mjd: Modified Julian Date of observation.\n
    :param fiber: SDSS fiber number.\n
    :param run: SDSS run number.\n
    :param rerun: SDSS rerun number.\n
    :param camcol: SDSS camera column.\n
    :param field: SDSS field number.\n
    :param obj: The object id within a field.\n
    :param dataRelease: SDSS data release string. Example: dataRelease='DR13'. Default value already set in SciServer.config.DataRelease
    :return: Returns a list containing the properties and metadata of the astronomical object found.
    :raises: Throws an exception if the HTTP request to the SkyServer API returns an error.
    :example: object = SkyServer.objectSearch(ra=258.25, dec=64.05)

    .. seealso:: SkyServer.sqlSearch, SkyServer.rectangularSearch, SkyServer.radialSearch.
    """

    url = get_url('SkyServerWS/SearchTools/ObjectSearch?query=LoadExplore&', data_release=dataRelease)

    url = pad_url(url, format='json', objId=objId, specObjId=specObjId, apogee_id=apogee_id, apstar_id=apstar_id,
                  ra=ra, dec=dec, plate=plate, mjd=mjd, fiber=fiber, run=run, rerun=rerun, camcol=camcol,
                  field=field, obj=obj, taskname='objectSearch')

    response = send_request(url, errmsg='Error when doing an object search.', stream=True)
    r = response.json()
    return r
