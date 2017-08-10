# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 13:28:13
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 10:11:56

from __future__ import print_function, division, absolute_import
from sciserver import scidrive
import pytest
import sys
import os

SciDrive_Directory = "/SciScriptPython"
SciDrive_FileName = "TestFile.csv"
SciDrive_FileContent = "Column1,Column2\n4.5,5.5\n"


@pytest.fixture()
def noscidrive():
    responseDelete = scidrive.delete(SciDrive_Directory)
    assert responseDelete is True
    yield responseDelete
    # delete again after test is done
    responseDelete = scidrive.delete(SciDrive_Directory)


@pytest.fixture()
def newfile():
    if (sys.version_info > (3, 0)):
        file = open(SciDrive_FileName, "w")
    else:
        file = open(SciDrive_FileName, "wb")
    file.write(SciDrive_FileContent)
    file.close()
    isfile = os.path.isfile(SciDrive_FileName)
    assert isfile is True
    yield isfile
    os.remove(SciDrive_FileName)


@pytest.mark.usefixtures('token')
class TestSciDrive(object):

    def test_createContainer_directoryList_delete(self, noscidrive):
        responseCreate = scidrive.createContainer(SciDrive_Directory)
        assert responseCreate is True

        dirList = scidrive.directoryList(SciDrive_Directory)
        assert dirList["path"].__contains__(SciDrive_Directory) is True

    def test_publicUrl(self, noscidrive):
        responseCreate = scidrive.createContainer(SciDrive_Directory)
        url = scidrive.publicUrl(SciDrive_Directory)
        isUrl = url.startswith("http")
        assert responseCreate is True
        assert isUrl is True

    def test_upload(self, newfile, noscidrive):
        path = SciDrive_Directory + "/" + SciDrive_FileName
        responseUpload = scidrive.upload(path=path, localFilePath=SciDrive_FileName)
        assert responseUpload is not None
        assert responseUpload['path'] == path

    @pytest.mark.parametrize('datatype, outformat',
                             [('file', 'StringIO'),
                              ('data', 'text')])
    def test_download(self, newfile, noscidrive, datatype, outformat):
        # open a file in Python 2 or 3

        path = SciDrive_Directory + "/" + SciDrive_FileName

        if datatype == 'file':
            responseUpload = scidrive.upload(path=path, localFilePath=SciDrive_FileName)
            stringio = scidrive.download(path=path, outformat=outformat)
            data = fileContent = stringio.read()
        elif datatype == 'data':
            responseUpload = scidrive.upload(path=path, data=SciDrive_FileContent)
            data = scidrive.download(path=path, outformat=outformat)

        assert responseUpload["path"] == path
        assert data == SciDrive_FileContent
