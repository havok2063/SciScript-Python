# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-30 14:58:30
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-09-01 11:35:52

from __future__ import print_function, division, absolute_import
from sciserver import config
from sciserver.utils import checkAuth, send_request, Task
from sciserver.casjobs import CasJobs
import os
import json
import time
import pandas


# Questions
# what units are startTime and submissionTime in job ? (missing decimal point)
# add a runTime or endTime
# can get a list of (finished | pending | running | errored) jobs?
# fix the SUCCES typo
# what is meant to be difference between FINISHED and (success/errored/canceled)
# add status for running?
#
# benchmarck queries 1 and 3 fail both in/out compute (u'Job failed. ERROR: canceling statement due to user request')
#
# login in compute - do stuff in notebook - token times out
# login again but don't open new container..use open notebook without having to close it
# token streamlines between compute and outside compute?
#
# tried logging in and getting a token from Airport Wifi Chicago O'Hare
# SSLError: ("bad handshake: SysCallError(54, 'ECONNRESET')",)

STATUS_INFO = {1: "PENDING", 2: "QUEUED", 4: "ACCEPTED", 8: "STARTED",
               16: "FINISHED", 32: "SUCCES", 64: "ERROR", 128: "CANCELED"}
STATUS_CODES = {v: k for k, v in STATUS_INFO.items()}


class Job(object):
    ''' This class represents a Compute Job '''

    def __init__(self, jobinfo, verbose=None):
        assert isinstance(jobinfo, dict), 'Job Info must be a dictionary object'
        # set class attributes
        for key, val in jobinfo.items():
            self.__setattr__(key, val)
        self.code = jobinfo.get('status', None)
        self.status = STATUS_INFO[self.code]
        self.error_message = None
        self.verbose = verbose

    def is_finished(self):
        return self.code >= STATUS_CODES['FINISHED']

    def to_dict(self):
        return self.__dict__

    def check_error(self):
        if self.status in ['ERROR', 'CANCELED']:
            self.error_message = self.messages[0]['content']
            if self.verbose:
                print('Job error message: {0}'.format(self.error_message))
        return self.error_message


class Compute(object):
    ''' This class contains methods for interacting with SciServer Compute Job system '''

    def __init__(self):
        self.computeURL = config.computeURL
        self.workspacePath = config.computeWorkspace
        self.jobsURL = os.path.join(self.computeURL, 'jobm/rest')
        self.job = None

    @checkAuth
    def retrieveDomains(self):
        ''' retrieve a list of domains '''
        jobdomains = 'computedomains/rdb'
        domainurl = os.path.join(self.jobsURL, jobdomains)

        response = send_request(domainurl, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving compute domains')
        if response.ok:
            jsonres = json.loads(response.content.decode())
            return jsonres

    def getJobs(self, status=None):
        ''' Get a list of all jobs '''
        joburl = 'jobs'
        url = os.path.join(self.jobsURL, joburl)
        response = send_request(url, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving jobs')
        if response.ok:
            jobjson = json.loads(response.content.decode())
            joblist = [Job(j) for j in jobjson]
            if status:
                joblist = [j for j in joblist if j.status == status]
            return joblist

    def getJob(self, jobid):
        ''' get a job '''
        joburl = 'jobs/{0}'.format(jobid)
        url = os.path.join(self.jobsURL, joburl)

        response = send_request(url, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving compute jobs')
        if response.ok:
            jobjson = json.loads(response.content.decode())
            return Job(jobjson)

    def getJobStatus(self, jobid):
        ''' Retrieve status of job '''
        self.job = self.getJob(jobid)
        return (self.job.code, self.job.status)

    def isJobFinished(self, jobid):
        ''' Returns true if job is finished '''
        self.job = self.getJob(jobid)
        return self.job.is_finished()

    @checkAuth
    def waitFor(self, jobid):
        ''' wait for the job to finish '''
        while not self.isJobFinished(jobid):
            print('Wait [{0}] ... '.format(self.job.status))
            time.sleep(1)
        else:
            print('Job [{0}]'.format(self.job.status))
            return self.job

    def _create_job_input(self, sql, context="manga", domainid=1, filename='result.csv',
                          file_type='FILE_CSV'):
        ''' creates a job dictionary '''
        job = {"inputSql": sql,
               "targets": [
                   {
                       "location": filename,
                       "type": file_type,
                       "resultNumber": 1
                   }],
               "databaseContextName": context,
               "rdbDomainId": domainid
               }
        return job

    @checkAuth
    def submitQuery(self, sql, context="manga", domainid=1, filename='result.csv', file_type='FILE_CSV'):
        ''' submit a compute sql query '''
        url = os.path.join(self.jobsURL, 'jobs/rdb')

        job = self._create_job_input(sql, context=context, domainid=domainid, filename=filename, file_type=file_type)

        data = json.dumps(job)

        response = send_request(url, reqtype='post', data=data, content_type='application/json',
                                errmsg='Error when submitting compute query', acceptHeader='application/json')
        if response.ok:
            jdata = json.loads(response.content.decode())
            jobid = jdata['id']
            return jobid

    def loadDataFrame(self):
        ''' Load the data into a Pandas Dataframe (works only on compute for now) '''
        resultsFolder = self.job['resultsFolderURI'].lstrip(os.path.sep)
        targetLoc = self.job['targets'][0]['location'].lstrip(os.path.sep)
        location = '{0}{1}{2}'.format(self.workspacePath, resultsFolder, targetLoc)
        df = pandas.read_csv(location)
        return df

    def retrieveData(self):
        ''' Retrieve data from the server '''
        alpha01URL = config.sciserverURL
        resultsFolder = self.job['resultsFolderURI'].lstrip(os.path.sep)
        targetLoc = self.job['targets'][0]['location'].lstrip(os.path.sep)
        datalink = os.path.join('fileservice/api/data', resultsFolder, targetLoc)
        fileURL = os.path.join(alpha01URL, datalink)

        response = send_request(fileURL, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving Job Results')
        if response.ok:
            return response.content

    def upload(self, tableName, context='MyDB'):
        ''' Upload a csv data file into user mydb '''
        csvdata = self.retrieveData()
        cas = CasJobs()
        cas.uploadCSVDataToTable(csvdata, tableName, context="MyDB")


