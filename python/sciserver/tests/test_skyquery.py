# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 11:38:53
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 10:37:04

from __future__ import print_function, division, absolute_import
import pytest

SkyQuery_TestTableName = "TestTable_SciScript_R"
SkyQuery_TestTableCSV = u"Column1,Column2\n4.5,5.5\n"
SkyQuery_TestTableCSVdownloaded = "#ID,Column1,Column2\n1,4.5,5.5\n"
SkyQuery_Query = "select 4.5 as Column1, 5.5 as Column2"


@pytest.mark.usefixtures("token")
class TestSkyQuerySubmit(object):

    def test_listqueues(self, skquery):
        queueList = skquery.listQueues()
        assert queueList is not None

    @pytest.mark.parametrize('qtype', [('quick'), ('long')])
    def test_getQueueInfo(self, skquery, qtype):
        queueInfo = skquery.getQueueInfo(qtype)
        assert queueInfo is not None

    def test_submitJob(self, skquery):
        jobId = skquery.submitJob(query=SkyQuery_Query, queue="quick")
        assert jobId is not None
        assert jobId is not ""

    def test_getJobStatus(self, skquery):
        jobId = skquery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = skquery.getJobStatus(jobId=jobId)
        assert jobDescription is not None

    def test_waitForJob(self, skquery):
        jobId = skquery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = skquery.waitForJob(jobId=jobId, verbose=True)
        assert jobDescription["status"] == "completed"

    def test_cancelJob(self, skquery):
        isCanceled = skquery.cancelJob(skquery.submitJob(query=SkyQuery_Query, queue="long"))
        assert isCanceled is True


@pytest.fixture()
def droptable(skquery):
    try:
        result = skquery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB")
    except Exception as e:
        pass


@pytest.fixture()
def uploadtable(skquery, droptable):
    result = skquery.uploadTable(uploadData=SkyQuery_TestTableCSV,
                                 tableName=SkyQuery_TestTableName, datasetName="MyDB", informat="csv")
    assert result is True
    yield result
    result = None


@pytest.mark.usefixtures("token")
class TestSkyQueryTable(object):

    def test_uploadtable(self, skquery, droptable):
        result = skquery.uploadTable(uploadData=SkyQuery_TestTableCSV,
                                     tableName=SkyQuery_TestTableName, datasetName="MyDB", informat="csv")
        assert result is True

    def test_gettable(self, skquery, uploadtable):
        table = skquery.getTable(tableName=SkyQuery_TestTableName, datasetName="MyDB", top=10)
        assert SkyQuery_TestTableCSVdownloaded == table.to_csv(index=False)

    def test_gettableinfo(self, skquery, uploadtable):
        info = skquery.getTableInfo(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
        columns = skquery.listTableColumns(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
        assert info is not None
        assert columns is not None

    def test_droptable(self, skquery, uploadtable):
        result = skquery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB")
        assert result is True


@pytest.mark.usefixtures("token")
class TestSkyQueryGetDbInfo(object):

    @pytest.mark.parametrize('qtype', [('quick'), ('long')])
    def test_listJobs(self, skquery, qtype):
        jobsList = skquery.listJobs(qtype)
        assert jobsList is not None

    def test_listAllDatasets(self, skquery):
        datasets = skquery.listAllDatasets()
        assert datasets is not None

    def test_getDatasetInfo(self, skquery):
        info = skquery.getDatasetInfo("MyDB")
        assert info is not None

    def test_listDatasetTables(self, skquery):
        tables = skquery.listDatasetTables("MyDB")
        assert tables is not None

