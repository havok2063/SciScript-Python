# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:39:16
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 20:24:11

from __future__ import print_function, division, absolute_import
from io import StringIO, BytesIO
from sciserver import config, authentication
from sciserver.utils import checkAuth, send_request
import requests as requests
import json


@checkAuth
def createContainer(path):
    """
    Creates a container (directory) in SciDrive

    Parameters:
        path (str):
            The path of the directory in SciDrive

    Returns:
        True if the container was created successfully

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> response = SciDrive.createContainer("MyDirectory")

    See Also:
        SciDrive.upload.

    """

    containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                     'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                     'uri="vos://{0}!vospace/{1}">'
                     '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                     '</vos:node>'.format(config.SciDriveHost, path))
    url = '{0}/vospace-2.0/nodes/{1}'.format(config.SciDriveHost, path)
    data = str.encode(containerBody)

    response = send_request(url, reqtype='put', data=data, content_type='application/xml',
                            errmsg='Error when creating SciDrive container at {0}'.format(path))

    if response.ok:
        return True


@checkAuth
def upload(path, data="", localFilePath=""):
    """
    Uploads data or a local file into a SciDrive directory.

    Parameters:
        path (str):
            Desired filepath in SciDrive
        data (str):
            data content to be uploaded into SciDrive. If the localFilePath parameter is set,
            then the local file will be uploaded instead.
        localFilePath (str):
            path to the local file to be uploaded

    Returns:
        A JSON object with the attributes of the uploaded file.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> response = SciDrive.upload("/SciDrive/path/to/file.csv", localFilePath="/local/path/to/file.csv")

    See Also:
        SciDrive.createContainer

    """

    url = config.SciDriveHost + '/vospace-2.0/1/files_put/dropbox/' + path

    if localFilePath:
        with open(localFilePath, 'rb') as file:
            data = file
            errmsg = 'Error when uploading local file {0} to SciDrive path {1}'.format(localFilePath, path)
            response = send_request(url, reqtype='put', data=data, stream=True, errmsg=errmsg)
    else:
        data = data
        errmsg = 'Error when uploading data to SciDrive path {0}'.format(path)
        response = send_request(url, reqtype='put', data=data, stream=True, errmsg=errmsg)

    if response.ok:
        return json.loads(response.content.decode())


@checkAuth
def publicUrl(path):
    """
    Gets the public URL of a file (or directory) in SciDrive.

    Parameters:
        path (str):
            The path of the file or directory in SciDrive

    Returns:
        fileUrl (str):
            the URL of a file in SciDrive

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> url = SciDrive.publicUrl("path/to/SciDrive/file.csv")

    See Also:
        SciDrive.upload.

    """

    url = '{0}/vospace-2.0/1/media/sandbox/{1}'.format(config.SciDriveHost, path)

    response = send_request(url, errmsg='Error when getting the public URL of SciDrive file {0}'.format(path))
    if response.ok:
        jsonRes = json.loads(response.content.decode())
        fileUrl = jsonRes["url"]
        return fileUrl


@checkAuth
def directoryList(path=""):
    """
    Gets the contents and metadata of a SciDrive directory (or file).

    Parameters:
        path (str):
            The path of the file or directory in SciDrive

    Returns:
        jsonRes (dict):
            a dictionary containing info and metadata of the directory (or file).

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> dirList = SciDrive.directoryList("path/to/SciDrive/directory")

    See Also:
        SciDrive.upload, SciDrive.download

    """

    url = "{0}/vospace-2.0/1/metadata/sandbox/{1}?list=True&path={1}".format(config.SciDriveHost, path)

    response = send_request(url, errmsg='Error when getting the public URL of SciDrive file {0}'.format(path))
    if response.ok:
        jsonRes = json.loads(response.content.decode())
        return jsonRes


@checkAuth
def download(path, outformat="text", localFilePath=""):
    """ Downloads a file or directory from SciDrive

    Downloads a file (directory) from SciDrive into the local file system,
    or returns the file conetent as an object in several formats.

    Parameters:
        path (str):
            The path of the file or directory in SciDrive
        outformat (str):
            Format of the returned data.  Can be "StringIO" (io.StringIO object containing readable text),
            "BytesIO" (io.BytesIO object containing readable binary data),
            "response" ( the HTTP response as an object of class requests.Response)
            or "text" (a text string). If the parameter 'localFilePath' is defined,
            then the 'format' parameter is not used and the file is downloaded to the local file system instead.
        localFilePath (str):
            local path of the file to be downloaded.  If defined, then outformat is not used.

    Returns:
        bool: True if 'localFilePath' parameter is defined, and the file is downloaded successfully

        If the 'localFilePath' is not defined, then the type of the returned object
        depends on the value of the 'outformat' parameter
        (either io.StringIO, io.BytesIO, requests.Response or string).

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> csvString = SciDrive.download("path/to/SciDrive/file.csv", format="text")

    See Also:
        SciDrive.upload

    """

    fileUrl = publicUrl(path)

    response = send_request(fileUrl, stream=True,
                            errmsg='Error when downloading SciDrive file {0}'.format(path))

    if response.ok:
        if localFilePath is not None and localFilePath != "":
            bytesio = BytesIO(response.content)
            theFile = open(localFilePath, "w+b")
            theFile.write(bytesio.read())
            theFile.close()
            return True
        else:
            if outformat is not None and outformat != "":
                if outformat == "StringIO":
                    return StringIO(response.content.decode())
                elif outformat == "text":
                    return response.content.decode()
                elif outformat == "BytesIO":
                    return BytesIO(response.content)
                elif outformat == "response":
                    return response
                else:
                    raise Exception("Unknown format {0} when trying to download SciDrive file {1}.".format(outformat, path))
            else:
                raise Exception("Wrong format parameter value")


@checkAuth
def delete(path):
    """
    Deletes a file or container (directory) in SciDrive.

    Parameters:
        path (str):
            The path of the file or directory in SciDrive

    Returns:
        True if the file or container (directory) was deleted successfully.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SciDrive API returns an error.

    Example:
        >>> response = SciDrive.delete("path/to/SciDrive/file.csv")

    See Also:
        SciDrive.upload

    """

    containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                     'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                     'uri="vos://{0}!vospace/{1}">'
                     '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                     '</vos:node>'.format(config.SciDriveHost, path))
    url = config.SciDriveHost + '/vospace-2.0/nodes/' + path
    data = str.encode(containerBody)

    response = send_request(url, reqtype='delete', data=data, content_type='application/xml',
                            errmsg='Error when deleting {0} in SciDrive'.format(path))
    if response.ok:
        return True
