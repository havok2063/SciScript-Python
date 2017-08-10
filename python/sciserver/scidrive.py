# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 15:39:16
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 10:09:42

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

    :param path: path of the directory in SciDrive.
    :return: Returns True if the container (directory) was created successfully.
    :raises: Throws an exception if the user is not logged into SciServer
    (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: response = SciDrive.createContainer("MyDirectory")

    .. seealso:: SciDrive.upload.
    """
    #token = authentication.getToken()
    # if token is not None and token != "":
    containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                     'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                     'uri="vos://{0}!vospace/{1}">'
                     '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                     '</vos:node>'.format(config.SciDriveHost, path))
    url = '{0}/vospace-2.0/nodes/{1}'.format(config.SciDriveHost, path)
    data = str.encode(containerBody)
    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/xml'}

    # res = requests.put(url, data=data, headers=headers)
    # if res.status_code < 200 or res.status_code >= 300:
    #     raise Exception("Error when creating SciDrive container at {0}."
    #                     "Http Response from SciDrive API returned status code {1}:"
    #                     "\n {2}".format(path, res.status_code, res.content.decode()))

    response = send_request(url, reqtype='put', data=data, content_type='application/xml',
                            errmsg='Error when creating SciDrive container at {0}'.format(path))

    if response.ok:
        return True
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def upload(path, data="", localFilePath=""):
    """
    Uploads data or a local file into a SciDrive directory.

    :param path: desired file path in SciDrive (string).
    :param data: data to be uploaded into SciDrive. If the 'localFilePath' parameter is set, then the local file will be uploaded instead.
    :param localFilePath: path to the local file to be uploaded (string).
    :return: Returns an object with the attributes of the uploaded file.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: response = SciDrive.upload("/SciDrive/path/to/file.csv", localFilePath="/local/path/to/file.csv")

    .. seealso:: SciDrive.createContainer
    """
    #token = authentication.getToken()
    #if token is not None and token != "":
    url = config.SciDriveHost + '/vospace-2.0/1/files_put/dropbox/' + path
    # headers = {'X-Auth-Token': token}
    # if(localFilePath != ""):
    #     with open(localFilePath, "rb") as file:
    #         res = requests.put(url, data=file, headers=headers, stream=True)
    # else:
    #     res = requests.put(url, data=data, headers=headers, stream=True)

    # if res.status_code != 200:
    #     if (localFilePath is not None):
    #         raise Exception("Error when uploading local file {0} to SciDrive path {1}."
    #                         "Http Response from SciDrive API returned status code {2}:"
    #                         "\n {3}".format(localFilePath, path, res.status_code, res.content.decode()))
    #     else:
    #         raise Exception("Error when uploading data to SciDrive path {0}."
    #                         "Http Response from SciDrive API returned status code {1}:"
    #                         "\n {2}".format(path, res.status_code, res.content.decode()))

    if localFilePath:
        with open(localFilePath, 'rb') as file:
            data = file
            errmsg = 'Error when uploading local file {0} to SciDrive path {1}'.format(localFilePath, path)
            response = send_request(url, reqtype='put', data=data, stream=True, errmsg=errmsg)
    else:
        data = data
        errmsg = 'Error when uploading data to SciDrive path {0}'.format(path)
        response = send_request(url, reqtype='put', data=data, stream=True, errmsg=errmsg)

    #response = send_request(url, reqtype='put', data=data, stream=True, errmsg=errmsg)
    if response.ok:
        return json.loads(response.content.decode())
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def publicUrl(path):
    """
    Gets the public URL of a file (or directory) in SciDrive.

    :param path: path of the file (or directory) in SciDrive.
    :return: URL of a file in SciDrive (string).
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: url = SciDrive.publicUrl("path/to/SciDrive/file.csv")

    .. seealso:: SciDrive.upload
    """
    #token = authentication.getToken()
    #if token is not None and token != "":

    url = '{0}/vospace-2.0/1/media/sandbox/{1}'.format(config.SciDriveHost, path)
    # headers = {'X-Auth-Token': token}
    # res = requests.get(url, headers=headers)
    # if res.status_code != 200:
    #     raise Exception("Error when getting the public URL of SciDrive file {0}. "
    #                     "Http Response from SciDrive API returned status code {1}:"
    #                     "\n {2}".format(path, res.status_code, res.content.decode()))

    response = send_request(url, errmsg='Error when getting the public URL of SciDrive file {0}'.format(path))
    if response.ok:
        jsonRes = json.loads(response.content.decode())
        fileUrl = jsonRes["url"]
        return fileUrl

    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def directoryList(path=""):
    """
    Gets the contents and metadata of a SciDrive directory (or file).

    :param path: path of the directory (or file ) in SciDrive.
    :return: a dictionary containing info and metadata of the directory (or file).
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: dirList = SciDrive.directoryList("path/to/SciDrive/directory")

    .. seealso:: SciDrive.upload, SciDrive.download
    """
    #token = authentication.getToken()
    #if token is not None and token != "":

    url = "{0}/vospace-2.0/1/metadata/sandbox/{1}?list=True&path={1}".format(config.SciDriveHost, path)
    # headers = {'X-Auth-Token': token}
    # res = requests.get(url, headers=headers)
    # if res.status_code != 200:
    #     raise Exception("Error when getting the public URL of SciDrive file {0}. "
    #                     "Http Response from SciDrive API returned status code {1}:"
    #                     "\n {2}".format(path, res.status_code, res.content.decode()))

    response = send_request(url, errmsg='Error when getting the public URL of SciDrive file {0}'.format(path))
    if response.ok:
        jsonRes = json.loads(response.content.decode())
        return jsonRes

    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def download(path, outformat="text", localFilePath=""):
    """
    Downloads a file (directory) from SciDrive into the local file system, or returns the file conetent as an object in several formats.

    :param path: path of the file (or directory) in SciDrive.
    :param format: type of the returned object. Can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "text" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :param localFilePath: local path of the file to be downloaded. If 'localFilePath' is defined, then the 'format' parameter is not used.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: csvString = SciDrive.download("path/to/SciDrive/file.csv", format="text");

    .. seealso:: SciDrive.upload
    """
    #token = authentication.getToken()
    #if token is not None and token != "":

    fileUrl = publicUrl(path)
    # res = requests.get(fileUrl, stream=True)
    # if res.status_code != 200:
    #     raise Exception("Error when downloading SciDrive file {0}. "
    #                     "Http Response from SciDrive API returned status code {1}:"
    #                     "\n {2}".format(path, res.status_code, res.content.decode()))

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

    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def delete(path):
    """
    Deletes a file or container (directory) in SciDrive.

    :param path: path of the file or container (directory) in SciDrive.
    :return: Returns True if the file or container (directory) was deleted successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
        :example: response = SciDrive.delete("path/to/SciDrive/file.csv")

    .. seealso:: SciDrive.upload.
    """
    #token = authentication.getToken()
    #if token is not None and token != "":
    containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                     'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                     'uri="vos://{0}!vospace/{1}">'
                     '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                     '</vos:node>'.format(config.SciDriveHost, path))
    url = config.SciDriveHost + '/vospace-2.0/nodes/' + path
    data = str.encode(containerBody)
    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/xml'}
    # res = requests.delete(url, data=data, headers=headers)
    # if res.status_code < 200 or res.status_code >= 300:
    #     raise Exception("Error when deleting {0} in SciDrive."
    #                     "Http Response from SciDrive API returned status code {1}:"
    #                     "\n {2}".format(path, res.status_code, res.content.decode()))

    response = send_request(url, reqtype='delete', data=data, content_type='application/xml',
                            errmsg='Error when deleting {0} in SciDrive'.format(path))
    if response.ok:
        return True
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")
