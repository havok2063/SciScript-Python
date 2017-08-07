# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 13:28:13
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 13:51:03

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
        # responseDelete = scidrive.delete(SciDrive_Directory)
        isUrl = url.startswith("http")
        assert responseCreate is True
        assert isUrl is True
        # assert responseDelete is True

    def test_upload_download(self, noscidrive, newfile):
        # open a file in Python 2 or 3

        path = SciDrive_Directory + "/" + SciDrive_FileName
        responseUpload = scidrive.upload(path=path, localFilePath=SciDrive_FileName)

        stringio = scidrive.download(path=path, outformat="StringIO")
        fileContent = stringio.read()
        responseDelete = scidrive.delete(SciDrive_Directory)
        assert responseUpload["path"] == path
        assert fileContent == SciDrive_FileContent
        assert responseDelete is True

        responseUpload = scidrive.upload(path=path, data=SciDrive_FileContent)
        fileContent = scidrive.download(path=path, outformat="text")
        responseDelete = scidrive.delete(SciDrive_Directory)
        assert responseUpload["path"] == path
        assert fileContent == SciDrive_FileContent
        assert responseDelete is True

