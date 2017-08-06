# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 16:25:44
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-06 17:29:31

from __future__ import print_function, division, absolute_import
from io import StringIO, BytesIO
import requests
import pandas
import skimage.io
from sciserver import authentication, config


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
    if(dataRelease):
        if dataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/SqlSearch?'.format(config.SkyServerWSurl, dataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/SqlSearch?'.format(config.SkyServerWSurl)
    else:
        if config.DataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/SqlSearch?'.format(config.SkyServerWSurl, config.DataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/SqlSearch?'.format(config.SkyServerWSurl)

    url = '{0}format=csv&'.format(url)
    url = '{0}cmd={1}&'.format(url, sql)

    if config.isSciServerComputeEnvironment():
        url = "{0}TaskName=Compute.SciScript-Python.SkyServer.sqlSearch&".format(url)
    else:
        url = "{0}TaskName=SciScript-Python.SkyServer.sqlSearch&".format(url)

    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception("Error when executing a sql query. "
                        "Http Response from SkyServer API returned status code {0}:"
                        "\n {1}".format(response.status_code, response.content.decode()))

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
    if(dataRelease):
        if dataRelease != "":
            url = '{0}/{1}/SkyServerWS/ImgCutout/getjpeg?'.format(config.SkyServerWSurl, dataRelease)
        else:
            url = '{0}/SkyServerWS/ImgCutout/getjpeg?'.format(config.SkyServerWSurl)
    else:
        if config.DataRelease != "":
            url = '{0}/{1}/SkyServerWS/ImgCutout/getjpeg?'.format(config.SkyServerWSurl, config.DataRelease)
        else:
            url = '{0}/SkyServerWS/ImgCutout/getjpeg?'.format(config.SkyServerWSurl)

    url = '{0}ra={1}&'.format(url, ra)
    url = '{0}dec={1}&'.format(url, dec)
    url = '{0}scale={1}&'.format(url, scale)
    url = '{0}width={1}&'.format(url, width)
    url = '{0}height={1}&'.format(url, height)
    url = '{0}opt={1}&'.format(url, opt)
    url = '{0}query={1}&'.format(url, query)

    if config.isSciServerComputeEnvironment():
        url = "{0}TaskName=Compute.SciScript-Python.SkyServer.getJpegImgCutout&".format(url)
    else:
        url = "{0}TaskName=SciScript-Python.SkyServer.getJpegImgCutout&".format(url)

    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        if response.status_code == 404 or response.status_code == 500:
            raise Exception("Error when getting an image cutout. "
                            "Http Response from SkyServer API returned status code {0}:"
                            "\n {1}".format(response.status_code, response.reason))
        else:
            raise Exception("Error when getting an image cutout. "
                            "Http Response from SkyServer API returned status code {0}:"
                            "\n {1}".format(response.status_code, response.content.decode()))
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

    if(dataRelease):
        if dataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/RadialSearch?'.format(config.SkyServerWSurl, dataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/RadialSearch?'.format(config.SkyServerWSurl)
    else:
        if config.DataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/RadialSearch?'.format(config.SkyServerWSurl, config.DataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/RadialSearch?'.format(config.SkyServerWSurl)

    url = '{0}format=csv&'.format(url)
    url = '{0}ra={1}&'.format(url, ra)
    url = '{0}dec={1}&'.format(url, dec)
    url = '{0}radius={1}&'.format(url, radius)
    url = '{0}coordType={1}&'.format(url, coordType)
    url = '{0}whichPhotometry={1}&'.format(url, whichPhotometry)
    url = '{0}limit={1}&'.format(url, limit)

    if config.isSciServerComputeEnvironment():
        url = "{0}TaskName=Compute.SciScript-Python.SkyServer.radialSearch&".format(url)
    else:
        url = "{0}TaskName=SciScript-Python.SkyServer.radialSearch&".format(url)

    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception("Error when executing a radial search. "
                        "Http Response from SkyServer API returned status code {0}:"
                        "\n {1}".format(response.status_code, response.content.decode()))

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

    if(dataRelease):
        if dataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/RectangularSearch?'.format(config.SkyServerWSurl, dataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/RectangularSearch?'.format(config.SkyServerWSurl)
    else:
        if config.DataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/RectangularSearch?'.format(config.SkyServerWSurl, config.DataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/RectangularSearch?'.format(config.SkyServerWSurl)

    url = '{0}format=csv&'.format(url)
    url = '{0}min_ra={1}&'.format(url, min_ra)
    url = '{0}max_ra={1}&'.format(url, max_ra)
    url = '{0}min_dec={1}&'.format(url, min_dec)
    url = '{0}max_dec={1}&'.format(url, max_dec)
    url = '{0}coordType={1}&'.format(url, coordType)
    url = '{0}whichPhotometry={1}&'.format(url, whichPhotometry)
    url = '{0}limit={1}&'.format(url, limit)

    if config.isSciServerComputeEnvironment():
        url = "{0}TaskName=Compute.SciScript-Python.SkyServer.rectangularSearch&".format(url)
    else:
        url = "{0}TaskName=SciScript-Python.SkyServer.rectangularSearch&".format(url)

    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception("Error when executing a rectangular search. "
                        "Http Response from SkyServer API returned status code {0}:"
                        "\n {1}".format(response.status_code, response.content.decode()))

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

    if(dataRelease):
        if dataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/ObjectSearch?query=LoadExplore&'.format(config.SkyServerWSurl, dataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/ObjectSearch?query=LoadExplore&'.format(config.SkyServerWSurl)
    else:
        if config.DataRelease != "":
            url = '{0}/{1}/SkyServerWS/SearchTools/ObjectSearch?query=LoadExplore&'.format(config.SkyServerWSurl, config.DataRelease)
        else:
            url = '{0}/SkyServerWS/SearchTools/ObjectSearch?query=LoadExplore&'.format(config.SkyServerWSurl)

    url = '{0}format=json&'.format(url)
    if objId:
        url = '{0}objId={1}&'.format(url, objId)
    if specObjId:
        url = '{0}specObjId={1}&'.format(url, specObjId)
    if apogee_id:
        url = '{0}apogee_id={1}&'.format(url, apogee_id)
    elif apstar_id:
        url = '{0}apstar_id={1}&'.format(url, apstar_id)
    if ra:
        url = '{0}ra={1}&'.format(url, ra)
    if dec:
        url = '{0}dec={1}&'.format(url, dec)
    if plate:
        url = '{0}plate={1}&'.format(url, plate)
    if mjd:
        url = '{0}mjd={1}&'.format(url, mjd)
    if fiber:
        url = '{0}fiber={1}&'.format(url, fiber)
    if run:
        url = '{0}run={1}&'.format(url, run)
    if rerun:
        url = '{0}rerun={1}&'.format(url, rerun)
    if camcol:
        url = '{0}camcol={1}&'.format(url, camcol)
    if field:
        url = '{0}field={1}&'.format(url, field)
    if obj:
        url = '{0}obj={1}&'.format(url, obj)

    if config.isSciServerComputeEnvironment():
        url = "{0}TaskName=Compute.SciScript-Python.SkyServer.objectSearch&".format(url)
    else:
        url = "{0}TaskName=SciScript-Python.SkyServer.objectSearch&".format(url)

    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    token = authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    response = requests.get(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception("Error when doing an object search. "
                        "Http Response from SkyServer API returned status code {0}:"
                        "\n {1}".format(response.status_code, response.content.decode()))

    r = response.json()
    return r
