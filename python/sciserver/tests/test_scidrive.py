# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 13:28:13
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 10:18:08

from __future__ import print_function, division, absolute_import
import pytest
import sys
import os

SciDrive_Directory = "SciScriptPython"
SciDrive_FileName = "TestFile.csv"
SciDrive_FileContent = "Column1,Column2\n4.5,5.5\n"


@pytest.fixture()
def noscidrive(sci):
    responseDelete = sci.delete(SciDrive_Directory)
    assert responseDelete is True
    yield responseDelete
    # delete again after test is done
    responseDelete = sci.delete(SciDrive_Directory)


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

    def test_createContainer_directoryList_delete(self, sci, noscidrive):
        responseCreate = sci.createContainer(SciDrive_Directory)
        assert responseCreate is True

        dirList = sci.directoryList(SciDrive_Directory)
        assert dirList["path"].__contains__(SciDrive_Directory) is True

    def test_publicUrl(self, sci, noscidrive):
        responseCreate = sci.createContainer(SciDrive_Directory)
        url = sci.publicUrl(SciDrive_Directory)
        isUrl = url.startswith("http")
        assert responseCreate is True
        assert isUrl is True

    def test_upload(self, sci, newfile, noscidrive):
        path = os.path.join(SciDrive_Directory, SciDrive_FileName)
        responseUpload = sci.upload(path=path, localFilePath=SciDrive_FileName)
        assert responseUpload is not None
        assert path in responseUpload['path']

    @pytest.mark.parametrize('datatype, outformat',
                             [('file', 'StringIO'),
                              ('data', 'text')])
    def test_download(self, sci, newfile, noscidrive, datatype, outformat):
        # open a file in Python 2 or 3

        path = os.path.join(SciDrive_Directory, SciDrive_FileName)

        if datatype == 'file':
            responseUpload = sci.upload(path=path, localFilePath=SciDrive_FileName)
            stringio = sci.download(path=path, outformat=outformat)
            data = fileContent = stringio.read()
        elif datatype == 'data':
            responseUpload = sci.upload(path=path, data=SciDrive_FileContent)
            data = sci.download(path=path, outformat=outformat)

        assert path in responseUpload["path"]
        assert data == SciDrive_FileContent
