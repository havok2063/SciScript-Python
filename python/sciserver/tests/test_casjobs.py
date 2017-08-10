# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-07 14:10:11
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-09 23:39:09

from __future__ import print_function, division, absolute_import
from sciserver import casjobs
import pytest
import os
import pandas
from io import StringIO

CasJobs_TestTableName1 = "MyNewtable1"
CasJobs_TestTableName2 = "MyNewtable2"
CasJobs_TestDatabase = "MyDB"
CasJobs_TestQuery = "select 4 as Column1, 5 as Column2 "
CasJobs_TestTableCSV = u"Column1,Column2\n4,5\n"
CasJobs_TestFitsFile = "SciScriptTestFile.fits"
CasJobs_TestCSVFile = "SciScriptTestFile.csv"


@pytest.fixture()
def remove_query():
    csv = 'query'
    yield csv
    csv = casjobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", outformat="csv")
    csv = None


@pytest.mark.usefixtures('token')
class TestCasJobs(object):

    def test_getSchemaName(self):
        casJobsId = casjobs.getSchemaName()
        assert casJobsId is not None
        assert casJobsId != ""

    def test_getTables(self):
        tables = casjobs.getTables(context="MyDB")
        assert tables is not None

    def test_executeQuery(self):
        df = casjobs.executeQuery(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase, outformat="pandas")
        assert CasJobs_TestTableCSV == df.to_csv(index=False)

    def test_submitJob(self):
        jobId = casjobs.submitJob(sql=CasJobs_TestQuery + " into MyDB." + CasJobs_TestTableName1, context=CasJobs_TestDatabase)
        jobDescription = casjobs.waitForJob(jobId=jobId, verbose=True)
        df = casjobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName1, context="MyDB", outformat="csv")
        assert jobId is not None
        assert jobId != ""

    def test_getJobStatus(self):
        jobId = casjobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        jobDescription = casjobs.getJobStatus(jobId)
        assert jobDescription["JobID"] == jobId

    def test_cancelJob(self):
        jobId = casjobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        isCanceled = casjobs.cancelJob(jobId=jobId)
        assert isCanceled is True

    def test_waitForJob(self):
        jobId = casjobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        jobDescription = casjobs.waitForJob(jobId=jobId, verbose=True)
        assert jobDescription["Status"] >= 3

    def test_writeFitsFileFromQuery(self):
        result = casjobs.writeFitsFileFromQuery(fileName=CasJobs_TestFitsFile, queryString=CasJobs_TestQuery, context="MyDB")
        assert result is True
        assert os.path.isfile(CasJobs_TestFitsFile) is True
        os.remove(CasJobs_TestFitsFile)

    def test_getPandasDataFrameFromQuery(self):
        df = casjobs.getPandasDataFrameFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        assert df.to_csv(index=False) == CasJobs_TestTableCSV

    def test_getNumpyArrayFromQuery(self):
        array = casjobs.getNumpyArrayFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        newArray = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None).as_matrix()
        assert array.all() == newArray.all()

    def test_uploadPandasDataFrameToTable(self, remove_query):
        df = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None)
        result = casjobs.uploadPandasDataFrameToTable(dataFrame=df, tableName=CasJobs_TestTableName2, context="MyDB")
        table = casjobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", outformat="pandas")
        result2 = casjobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", outformat="csv")
        assert result is True
        assert table.all() == df.all()

    def test_uploadCSVDataToTable(self, remove_query):
        df = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None)
        result = casjobs.uploadCSVDataToTable(csvData=CasJobs_TestTableCSV, tableName=CasJobs_TestTableName2, context="MyDB")
        df2 = casjobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", outformat="pandas")
        result2 = casjobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", outformat="csv")
        assert result is True
        assert df.all() == df2.all()

