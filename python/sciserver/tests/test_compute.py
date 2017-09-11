# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-09-10 22:37:25
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-09-11 00:18:45

from __future__ import print_function, division, absolute_import
import pytest
import os
import pandas
from io import StringIO
from sciserver.compute import Job

Compute_TestTableName1 = "table1"
Compute_TestDatabase = "MyDB"
Compute_TestQuery = "SELECT z, sersic_n, petroflux_el FROM mangasampledb.nsa LIMIT 10"
Compute_TestResults = (u'z,sersic_n,petroflux_el\n0.021222278475761414,4.776151657104492,'
                       '{18.7879161834716797 30.7565364837646484 135.98065185546875 '
                       '566.685791015625 1144.0712890625 1671.892822265625 2225.009033203125}')
Compute_TestCSVFile = "testresults.csv"
Compute_Jobid = 421


@pytest.fixture(scope='session')
def job(comp):
    job = comp.getJob(Compute_Jobid)
    yield job
    job = None


@pytest.mark.usefixtures('token')
class TestCompute(object):

    def test_retrieveDomains(self, comp):
        domains = comp.retrieveDomains()
        assert domains is not None
        assert domains != []
        domain = domains[0]
        keys = domain.keys()
        assert 'name' in keys
        assert 'apiEndpoint' in keys
        assert 'id' in keys
        ids = [d['id'] for d in domains]
        assert set([6, 7]).issubset(set(ids))

    def test_getJobs(self, comp):
        jobs = comp.getJobs()
        assert jobs is not None
        assert isinstance(jobs[0], Job) is True

    def test_getJob(self, comp):
        job = comp.getJob(Compute_Jobid)
        assert isinstance(job, Job) is True
        assert job.id == Compute_Jobid
        assert job.status == 'SUCCES'

    def test_getJobStatus(self, comp):
        code, status = comp.getJobStatus(Compute_Jobid)
        assert code == 32
        assert status == 'SUCCES'

    def test_isJobFinished(self, comp):
        fin = comp.isJobFinished(Compute_Jobid)
        assert fin is True

    def test_submitQuery(self, comp):
        jobid = comp.submitQuery(Compute_TestQuery)
        assert jobid is not None
        assert jobid > Compute_Jobid

    def test_waitFor(self, comp, job):
        jobid = comp.submitQuery(Compute_TestQuery)
        newjob = comp.waitFor(jobid)
        assert job.inputSql == newjob.inputSql


@pytest.mark.usefixtures('token')
class TestJob(object):

    def test_job_finished(self, job):
        assert job.is_finished() is True

    def test_retrieveData(self, job):
        csv = job.retrieveData()
        row = '\n'.join(csv.split('\n', 2)[0:2])
        assert Compute_TestResults == row

    def test_pandas(self, job):
        df = job.loadDataFrame()
        testdf = pandas.read_csv(StringIO(Compute_TestResults))
        assert df.loc[:0]['z'][0] == testdf['z'][0]

