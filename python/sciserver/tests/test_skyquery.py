# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 11:38:53
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 12:00:24

from __future__ import print_function, division, absolute_import
from sciserver import skyquery
import pytest

SkyQuery_TestTableName = "TestTable_SciScript_R"
SkyQuery_TestTableCSV = u"Column1,Column2\n4.5,5.5\n"
SkyQuery_TestTableCSVdownloaded = "#ID,Column1,Column2\n1,4.5,5.5\n"
SkyQuery_Query = "select 4.5 as Column1, 5.5 as Column2"


#@pytest.mark.usefixtures("token")
class TestSkyQuerySubmit(object):

    def test_listqueues(self):
        queueList = skyquery.listQueues()
        assert queueList is not None

    @pytest.mark.parametrize('qtype', [('quick'), ('long')])
    def test_getQueueInfo(self, qtype):
        queueInfo = skyquery.getQueueInfo(qtype)
        assert queueInfo is not None

    def test_submitJob(self):
        jobId = skyquery.submitJob(query=SkyQuery_Query, queue="quick")
        assert jobId is not None
        assert jobId is not ""

    def test_getJobStatus(self):
        jobId = skyquery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = skyquery.getJobStatus(jobId=jobId)
        assert jobDescription is not None

    def test_waitForJob(self):
        jobId = skyquery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = skyquery.waitForJob(jobId=jobId, verbose=True)
        assert jobDescription["status"] == "completed"

    def test_cancelJob(self):
        isCanceled = skyquery.cancelJob(skyquery.submitJob(query=SkyQuery_Query, queue="long"))
        assert isCanceled is True


@pytest.fixture()
def droptable():
    try:
        result = skyquery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB")
    except Exception as e:
        pass


@pytest.fixture()
def uploadtable(droptable):
    result = skyquery.uploadTable(uploadData=SkyQuery_TestTableCSV,
                                  tableName=SkyQuery_TestTableName, datasetName="MyDB", outformat="csv")
    assert result is True
    yield result
    result = None


#@pytest.mark.usefixtures("token")
class TestSkyQueryTable(object):

    def test_uploadtable(self, droptable):
        result = skyquery.uploadTable(uploadData=SkyQuery_TestTableCSV,
                                      tableName=SkyQuery_TestTableName, datasetName="MyDB", outformat="csv")
        assert result is True

    def test_gettable(self, uploadtable):
        table = skyquery.getTable(tableName=SkyQuery_TestTableName, datasetName="MyDB", top=10)
        assert SkyQuery_TestTableCSVdownloaded == table.to_csv(index=False)

    def test_gettableinfo(self, uploadtable):
        info = skyquery.getTableInfo(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
        columns = skyquery.listTableColumns(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
        assert info is not None
        assert columns is not None

    def test_droptable(self, uploadtable):
        result = skyquery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB")
        assert result is True


#@pytest.mark.usefixtures("token")
class TestSkyQueryGetDbInfo(object):

    @pytest.mark.parametrize('qtype', [('quick'), ('long')])
    def test_listJobs(self, qtype):
        jobsList = skyquery.listJobs(qtype)
        assert jobsList is not None

    def test_listAllDatasets(self):
        datasets = skyquery.listAllDatasets()
        assert datasets is not None

    def test_getDatasetInfo(self):
        info = skyquery.getDatasetInfo("MyDB")
        assert info is not None

    def test_listDatasetTables(self):
        tables = skyquery.listDatasetTables("MyDB")
        assert tables is not None

