# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 13:28:13
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 10:52:53

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
def newfile(tmpdir):
    file = tmpdir.mkdir(SciDrive_Directory).join(SciDrive_FileName)
    file.write(SciDrive_FileContent)
    isfile = file.check(file=1)
    assert isfile is True
    yield file


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
        remotepath = os.path.join(SciDrive_Directory, SciDrive_FileName)
        localpath = str(newfile)
        responseUpload = sci.upload(path=remotepath, localFilePath=localpath)
        assert responseUpload is not None
        assert remotepath in responseUpload['path']

    @pytest.mark.parametrize('datatype, outformat',
                             [('file', 'StringIO'),
                              ('data', 'text')])
    def test_download(self, sci, newfile, noscidrive, datatype, outformat):
        # open a file in Python 2 or 3

        remotepath = os.path.join(SciDrive_Directory, SciDrive_FileName)
        localpath = str(newfile)

        if datatype == 'file':
            responseUpload = sci.upload(path=remotepath, localFilePath=localpath)
            stringio = sci.download(path=remotepath, outformat=outformat)
            data = fileContent = stringio.read()
        elif datatype == 'data':
            responseUpload = sci.upload(path=remotepath, data=SciDrive_FileContent)
            data = sci.download(path=remotepath, outformat=outformat)

        assert remotepath in responseUpload["path"]
        assert data == SciDrive_FileContent
