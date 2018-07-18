# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-30 14:58:30
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-10-16 15:45:23

from __future__ import print_function, division, absolute_import
from sciserver import config
from sciserver.utils import checkAuth, send_request, Task
from sciserver.casjobs import CasJobs
from sciserver.exceptions import SciServerError
from io import StringIO
import os
import json
import time
import pandas
import datetime
import re

# Questions
# add a runTime or endTime
# can get a list of (finished | pending | running | errored) jobs?
#
# login in compute - do stuff in notebook - token times out
# login again but don't open new container..use open notebook without having to close it
# token streamlines between compute and outside compute?
#
# tried logging in and getting a token from Airport Wifi Chicago O'Hare
# SSLError: ("bad handshake: SysCallError(54, 'ECONNRESET')",)

# option to load to mydb on backend when submit query
#   - backend results get pushed to file and mydb
# what does targets['type'] refer to?
# - in submitQuery - make new target_type for file and mydb?
#   - have tablename as option, tablename, filename, file_type?

STATUS_INFO = {1: "PENDING", 2: "QUEUED", 4: "ACCEPTED", 8: "STARTED",
               16: "FINISHED", 32: "SUCCESS", 64: "ERROR", 128: "CANCELED"}
STATUS_CODES = {v: k for k, v in STATUS_INFO.items()}


class Job(object):
    ''' This class represents a Compute Job '''

    def __init__(self, jobinfo, verbose=None):
        ''' A SciServer Compute Job

        Parameters:
            jobinfo (dict):
                A dictionary of job information.  Returned by the Compute System
            verbose (bool):
                If True, outputs more info to the screen

        '''
        assert isinstance(jobinfo, dict), 'Job Info must be a dictionary object'
        self._orig = jobinfo
        # set class attributes
        for key, val in jobinfo.items():
            self.__setattr__(key, val)
        self.code = jobinfo.get('status', None)
        self.status = STATUS_INFO[self.code]
        self.error_message = None
        self.verbose = verbose
        if self.code > STATUS_CODES['FINISHED']:
            self.set_datetimes()

    def __repr__(self):
        return '<Job(jobid={0.id}, status={0.status}, code={0.code})>'.format(self)

    @property
    def result_path(self):
        ''' Build the sub-path to the results file '''
        resultsFolder = self.resultsFolderURI.lstrip(os.path.sep)
        targetLoc = self.targets[0]['location'].lstrip(os.path.sep)
        resultlink = os.path.join(resultsFolder, targetLoc)
        return resultlink

    def is_finished(self):
        ''' Check if the job is finished or not '''
        return self.code >= STATUS_CODES['FINISHED']

    def to_dict(self):
        ''' Converts the Job to a dictionary '''
        return self.__dict__

    def check_error(self):
        ''' Checks the job for an error message '''
        if self.status in ['ERROR', 'CANCELED']:
            self.error_message = self.messages[0]['content']
            if self.verbose:
                print('Job error message: {0}'.format(self.error_message))
        return self.error_message

    def set_datetimes(self):
        ''' Converts job times into Python datetime objects '''

        # multiply by 1e-3 to convert times from milliseconds to seconds
        self.startTime = datetime.datetime.fromtimestamp(self.startTime * 1e-3)
        self.submissionTime = datetime.datetime.fromtimestamp(self.submissionTime * 1e-3)

    def loadDataFrame(self):
        ''' Load the data into a Pandas Dataframe

        Returns:
            A Pandas Dataframe of the results

        '''
        if config.isSciServerComputeEnvironment():
            location = '{0}{1}'.format(self.workspacePath, self.result_path)
            df = pandas.read_csv(location)
        else:
            csv = self.retrieveData()
            csv = csv.replace('{', '"{').replace('}', '}"')  # is this temporary?
            df = pandas.read_csv(StringIO(csv), index_col=None)
        return df

    def retrieveData(self):
        ''' Retrieve data from the server

        Returns:
            A string representation of the CSV results data file

        '''

        assert self.status == 'SUCCESS', 'Job must be successful to retrieve data'

        alpha01URL = config.sciserverURL
        datalink = os.path.join('fileservice/api/data', self.result_path)
        fileURL = os.path.join(alpha01URL, datalink)

        response = send_request(fileURL, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving Job Results')
        if response.ok:
            return response.content.decode()

    def upload(self, tableName, context='MyDB'):
        ''' Upload the job csv data into user mydb

        Uploads the Job results into MyDB using the SciServer
        CasJobs class.

        Parameters:
            tableName (str):
                The name of the table of create in MyDB
            context (str):
                The database name.  Default is MyDB

        '''
        csvdata = self.retrieveData()
        cas = CasJobs()
        cas.uploadCSVDataToTable(csvdata, tableName, context="MyDB")


class Compute(object):
    ''' This class contains methods for interacting with SciServer Compute Job system '''

    def __init__(self):
        self.computeURL = config.computeURL
        self.workspacePath = config.computeWorkspace
        self.jobsURL = os.path.join(self.computeURL, 'jobm/rest')
        self.job = None
        self.targets = []
        self.set_domains()

    def set_domains(self):
        ''' Set the domains for the short and long queues '''
        comp = re.compile('\(([a-z]+)\)')
        domains = self.retrieveDomains()
        try:
            self.domains = {comp.search(dom['name']).group(1): dom['id'] for dom in domains}
        except AttributeError as e:
            self.domains = None
        else:
            self.domains['quick'] = self.domains['short']

    @checkAuth
    def retrieveDomains(self):
        ''' Retrieve a list of domains

        Retrieve a list of compute domains.  These are the available
        domains to submit jobs to, indicated by 'id'

        Returns:
            A list of compute domains (as JSON dictionaries).

        '''
        jobdomains = 'computedomains/rdb'
        domainurl = os.path.join(self.jobsURL, jobdomains)

        response = send_request(domainurl, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving compute domains')
        if response.ok:
            jsonres = json.loads(response.content.decode())
            return jsonres

    def getJobs(self, status=None):
        ''' Get a list of all jobs

        Parameters:
            status (str):
                A specific status of jobs to return

        Returns:
            A list of SciServer Jobs with the given status

        '''
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
        ''' Get a job

        Get a compute job with a specific job id

        Parameters:
            jobid (int):
                The job id to request

        Returns:
            The SciServer Job with the requested id.

        '''
        joburl = 'jobs/{0}'.format(jobid)
        url = os.path.join(self.jobsURL, joburl)

        response = send_request(url, content_type='application/json', acceptHeader='application/json',
                                errmsg='Error when retrieving compute jobs')
        if response.ok:
            jobjson = json.loads(response.content.decode())
            return Job(jobjson)

    def getJobStatus(self, jobid):
        ''' Retrieve status of job

        Parameters:
            jobid (int):
                The job id to request

        Returns:
            A tuple of the job status code, and string status

        '''
        self.job = self.getJob(jobid)
        return (self.job.code, self.job.status)

    def isJobFinished(self, jobid):
        ''' Checks if job is finished

        Parameters:
            jobid (int):
                The job id to request

        Returns:
            True if the job has a status of FINISHED

        '''
        self.job = self.getJob(jobid)
        return self.job.is_finished()

    @checkAuth
    def waitFor(self, jobid):
        ''' Wait for the job to finish

        Parameters:
            jobid (int):
                The job id to request

        Returns:
            The SciServer Job

        '''
        while not self.isJobFinished(jobid):
            print('Wait [{0}] ... '.format(self.job.status))
            time.sleep(1)
        else:
            print('Job [{0}]'.format(self.job.status))
            return self.job

    def add_target(self, target_type, tablename='mytable', filename='results.csv', file_type='CSV'):
        ''' Add a target location for query results '''

        tt = target_type.upper()
        assert tt in ['FILE', 'TABLE'], 'target_type can only be FILE or TABLE'

        # existing types
        types = [t['type'] for t in self.targets]

        # add a table
        if tt == 'TABLE':
            tab_targ = {'location': tablename, 'type': tt, 'resultNumber': 1}
            if tt in types:
                tindex = types.index(tt)
                self.targets[tindex] = tab_targ
            else:
                self.targets.append(tab_targ)

        # add a file
        if tt == 'FILE':
            filetype = '{0}_{1}'.format(tt, file_type.upper())
            file_targ = {'location': filename, 'type': filetype, 'resultNumber': 1}
            if filetype in types:
                tindex = types.index(filetype)
                self.targets[tindex] = file_targ
            else:
                self.targets.append(file_targ)

    def _create_job_input(self, sql, context="manga", queue='quick', filename='results.csv',
                          file_type='CSV', target_type='FILE', tablename='mytable',
                          name='sciserver_query'):
        ''' creates a job dictionary '''

        # set the domainid based on the query queue
        assert queue in ['short', 'quick', 'long'], 'Query queue must be either short/quick or long'
        if self.domains:
            domainid = self.domains[queue]
        else:
            raise SciServerError('No domainid lookup found.  Check retrieveDomains for domain names.')

        # add the target for the results
        if not self.targets:
            self.add_target(target_type, tablename=tablename, filename=filename, file_type=file_type)

        job = {"inputSql": sql,
               "targets": self.targets,
               "databaseContextName": context,
               "rdbDomainId": domainid,
               "submitterDID": name
               }
        return job

    @checkAuth
    def submitQuery(self, sql, context="manga", queue='quick', target_type='FILE',
                    tablename='mytable', filename='results.csv', file_type='CSV',
                    name='sciserver_query'):
        ''' Submit a SQL query to compute

        Parameters:
            sql (str):
                The sql query string to submit
            context (str):
                The database to connect to
            domainId (int):
                The compute domain id to connect to
            target_type (str):
                Type of output results.  Either TABLE or FILE
            tablename (str):
                The MyDB tablename to save the results to
            filename (str):
                The filename of the query results
            file_type (str):
                The type of file to save the results as
            name (str):
                The name of the query to be submitted

        Returns:
            jobid (int):
                The job id of the submitted query

        '''
        url = os.path.join(self.jobsURL, 'jobs/rdb')

        job = self._create_job_input(sql, context=context, queue=queue, target_type=target_type,
                                     tablename=tablename, filename=filename, file_type=file_type, name=name)

        data = json.dumps(job)

        response = send_request(url, reqtype='post', data=data, content_type='application/json',
                                errmsg='Error when submitting compute query', acceptHeader='application/json')
        if response.ok:
            jdata = json.loads(response.content.decode())
            jobid = jdata['id']
            return jobid

