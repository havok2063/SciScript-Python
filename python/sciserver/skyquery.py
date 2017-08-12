# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 16:00:25
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-12 13:28:43

from __future__ import print_function, division, absolute_import
from io import StringIO
import json
import time
import pandas
from sciserver import config
from sciserver.utils import checkAuth, send_request


@checkAuth
def getJobStatus(jobId):
    """ Get the status of a job

    Gets the status of a job, as well as other related metadata
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        jobId (str):
            the ID of the job, which is obtained at the moment of submitting the job.

    Returns:
        dict: a dictionary with the job status and other related metadata.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> status = skyquery.getJobStatus(skyquery.submitJob("select 1 as foo"))

    See Also:
        SkyQuery.submitJob, SkyQuery.cancelJob

    """

    statusURL = '{0}/Jobs.svc/jobs/{1}'.format(config.SkyQueryUrl, jobId)

    response = send_request(statusURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when getting job status {0}'.format(jobId))
    if response.ok:
        r = response.json()
        return r['queryJob']


@checkAuth
def cancelJob(jobId):
    """ Cancels a job

    Cancels a single job (more info in http://www.voservices.net/skyquery).

    Parameters:
        jobId (str):
            the ID of the job, which is obtained at the moment of submitting the job.

    Returns:
        True if the job was cancelled successfully

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> isCanceled = SkyQuery.cancelJob(SkyQuery.submitJob("select 1 as foo"))

    See Also:
        SkyQuery.submitJob, SkyQuery.getJobStatus

    """

    statusURL = '{0}/Jobs.svc/jobs/{1}'.format(config.SkyQueryUrl, jobId)

    response = send_request(statusURL, reqtype='delete', content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when canceling job {0}'.format(jobId))

    if response.ok:
        r = response.json()
        try:
            status = r['queryJob']["status"]
            if status == 'canceled':
                return True
            else:
                return False
        except Exception as e:
            return False


@checkAuth
def listQueues():
    """ Retrieves a list of job queues

    Returns a list of all available job queues and
    related metadata (more info in http://www.voservices.net/skyquery).

    Returns:
        a list of all available job queues and related metadata

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> queueList = SkyQuery.listQueues()

    See Also:
        SkyQuery.getQueueInfo, SkyQuery.submitJob, SkyQuery.getJobStatus

    """

    jobsURL = '{0}/Jobs.svc/queues'.format(config.SkyQueryUrl)

    response = send_request(jobsURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when listing queues')
    if response.ok:
        r = response.json()
        return r['queues']


@checkAuth
def getQueueInfo(queue):
    """ Retrieves queue info

    Returns information about a particular job queue
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        queue (str):
            the name of the queue

    Returns:
        a dictionary containing information associated to the queue.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> queueInfo = SkyQuery.getQueueInfo('quick')

    See Also:
        SkyQuery.listQueues, SkyQuery.submitJob, SkyQuery.getJobStatus

    """

    jobsURL = '{0}/Jobs.svc/queues/{1}'.format(config.SkyQueryUrl, queue)

    response = send_request(jobsURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when getting queue info {0}'.format(queue))

    if response.ok:
        r = response.json()
        return r['queue']


@checkAuth
def submitJob(query, queue='quick'):
    """ Submit a job

    Submits a new job (more info in http://www.voservices.net/skyquery).

    Parameters:
        query (str):
            the sql query string
        queue (str):
            the name of the queue.  Can be 'quick' (quick job) or 'long' (long job). Default is quick.

    Returns:
        returns the jobId, unique identifier of the job.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> jobId = SkyQuery.submitJob('select 1 as foo', "quick")

    See Also:
        SSkyQuery.getJobStatus, SkyQuery.listQueues

    """

    jobsURL = '{0}/Jobs.svc/queues/{1}/jobs'.format(config.SkyQueryUrl, queue)

    body = {"queryJob": {"query": query}}
    data = json.dumps(body).encode()

    response = send_request(jobsURL, reqtype='post', data=data, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when submitting job on queue {0}'.format(queue))
    if response.ok:
        r = response.json()
        return r['queryJob']['guid']


def waitForJob(jobId, verbose=True):
    """ Wait for a running job to finish

    Queries the job status from SkyQuery every 2 seconds and waits
    for the SkyQuery job to be completed.

    Parameters:
        jobId (str):
            the ID of the job, which is obtained at the moment of submitting the job.
        verbose (bool):
            if True, prints 'wait' messages while the job is running.

    Returns:
        dict: a dictionary with the job status and other related metadata.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> skyquery.waitForJob(skyquery.submitJob("select 1"))

    See Also:
        SkyQuery.submitJob, SkyQuery.getJobStatus.

    """
    try:
        complete = False

        waitingStr = "Waiting..."
        back = "\b" * len(waitingStr)
        if verbose:
            print(waitingStr, end="")

        while not complete:
            if verbose:
                print(waitingStr, end="")
            jobDesc = getJobStatus(jobId)
            if jobDesc['status'] == 'completed':
                complete = True
                if verbose:
                    print("Done!")
            else:
                time.sleep(2)

        return jobDesc
    except Exception as e:
        raise e


@checkAuth
def listJobs(queue="quick"):
    """ Lists the jobs in the queue

    Lists the jobs in the queue in descending order by submission time.
    Only jobs of the authenticated user are listed
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        queue (str):
            the name of the queue.  Can be 'quick' (quick job) or 'long' (long job). Default is quick.

    Returns:
        list: a list of job definitions

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> jobsList = SkyQuery.listJobs('quick')

    See Also:
        SkyQuery.getJobStatus, SkyQuery.listQueues

    """

    jobsURL = '{0}/Jobs.svc/queues/{1}/jobs?'.format(config.SkyQueryUrl, queue)

    response = send_request(jobsURL, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when listing jobs on queue {0}'.format(queue))
    if response.ok:
        r = response.json()
        return r['jobs']


@checkAuth
def listAllDatasets():
    """ List all the datasets

    Lists all available datasets (more info in http://www.voservices.net/skyquery).

    Returns:
        list: a list of job definitions

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> datasets = SkyQuery.listAllDatasets()

    See Also:
        SkyQuery.listQueues, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables,
        SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable,
        SkyQuery.dropTable

    """

    schemaURL = '{0}/Schema.svc/datasets'.format(config.SkyQueryUrl)

    response = send_request(schemaURL, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when listing all datasets')
    if response.ok:
        r = response.json()
        return r['datasets']


@checkAuth
def getDatasetInfo(datasetName="MyDB"):
    """
    Gets information related to a particular dataset
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        datasetName (str):
            the name of the dataset

    Returns:
        dict: a dictionary containing the dataset definition

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> info = SkyQuery.getDatasetInfo("MyDB")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.listDatasetTables,
        SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable,
        SkyQuery.dropTable

    """

    schemaURL = '{0}/Schema.svc/datasets/{1}'.format(config.SkyQueryUrl, datasetName)

    response = send_request(schemaURL, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when getting info from dataset {0}'.format(datasetName))
    if response.ok:
        r = response.json()
        return r


def listDatasetTables(datasetName="MyDB"):
    """ Return a list of all tables

    Returns a list of all tables within a dataset
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        datasetName (str):
            the name of the dataset

    Returns:
        list: a list containing the tables and associated descriptions/metadata.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> tables = SkyQuery.listDatasetTables("MyDB")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable,
        SkyQuery.dropTable

    """

    url = '{0}/Schema.svc/datasets/{1}/tables'.format(config.SkyQueryUrl, datasetName)

    response = send_request(url, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when listing tables in dataset {0}'.format(datasetName))
    if response.ok:
        r = response.json()
        return r['tables']


@checkAuth
def getTableInfo(tableName, datasetName="MyDB"):
    """ Returns table information

    Returns info about a particular table belonging to a dataset
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        tableName (str):
            the name of the table in a dataset
        datasetName (str):
            the name of the dataset

    Returns:
        dict: a dictionary containing the tables properties and associated info/metadata.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> info = SkyQuery.getTableInfo("myTable", datasetName="MyDB")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.listDatasetTables, SkyQuery.listTableColumns, SkyQuery.getTable,
        SkyQuery.dropTable

    """

    url = '{0}/Schema.svc/datasets/{1}/tables/{2}'.format(config.SkyQueryUrl, datasetName, tableName)

    response = send_request(url, content_type='application/json',
                            acceptHeader='application/json',
                            errmsg='Error when getting info of table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        r = response.json()
        return r


@checkAuth
def listTableColumns(tableName, datasetName="MyDB"):
    """ Returns columns of a table

    Returns a list of all columns in a table belonging to a particular
    dataset (more info in http://www.voservices.net/skyquery).

    Parameters:
        tableName (str):
            the name of the table in a dataset
        datasetName (str):
            the name of the dataset

    Returns:
        list: a list containing the columns and associated descriptions.

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> columns = SkyQuery.listTableColumns("myTable", datasetName="MyDB")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.getTable,
        SkyQuery.dropTable

    """

    url = '{0}/Schema.svc/datasets/{1}/tables/{2}/columns'.format(config.SkyQueryUrl, datasetName, tableName)

    response = send_request(url, content_type='application/json',
                            acceptHeader='application/json',
                            errmsg='Error when listing columns of table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        r = response.json()
        return r['columns']


# Data:

@checkAuth
def getTable(tableName, datasetName="MyDB", top=None):
    """ Return a table

    Returns a dataset table as a pandas DataFrame
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        tableName (str):
            the name of the table in a dataset
        datasetName (str):
            the name of the dataset
        top (int):
            the top number of rows in the table

    Returns:
        the table as a Pandas dataframe

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> table = SkyQuery.getTable("myTable", datasetName="MyDB", top=10)

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.listTableColumns,
        SkyQuery.dropTable

    """

    url = '{0}/Data.svc/{1}/{2}'.format(config.SkyQueryUrl, datasetName, tableName)
    if top is not None and top != "":
        url = url + '?top=' + str(top)

    response = send_request(url, content_type='application/json',
                            acceptHeader='application/json', stream=True,
                            errmsg='Error when getting table {0} from dataset {1}'.format(tableName, datasetName))
    if response.ok:
        r = response.content.decode()
        return pandas.read_csv(StringIO(r), sep="\t")


@checkAuth
def dropTable(tableName, datasetName="MyDB"):
    """ Drops a db table

    Drops (deletes) a table from the user database
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        tableName (str):
            the name of the table in a dataset
        datasetName (str):
            the name of the dataset

    Returns:
        True if the table was dropped successfully

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> result = SkyQuery.dropTable("myTable", datasetName="MyDB")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.listTableColumns,
        SkyQuery.getTable

    """

    url = '{0}/Data.svc/{1}/{2}'.format(config.SkyQueryUrl, datasetName, tableName)

    response = send_request(url, reqtype='delete', content_type='application/json',
                            acceptHeader='application/json',
                            errmsg='Error when dropping table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        return True


@checkAuth
def uploadTable(uploadData, tableName, datasetName="MyDB", informat="csv"):
    """ Uploads a table

    Uploads a data table into a database
    (more info in http://www.voservices.net/skyquery).

    Parameters:
        uploadData (str):
            the table data in CSV string format
        tableName (str):
            the name of the table in a dataset
        datasetName (str):
            the name of the dataset
        informat (str):
            The format of the input data.  Default is 'csv'.

    Returns:
        True if the table was uploaded successfully

    Raises:
        SciServerAPIError: Throws an exception if the HTTP request to the SkyQuery API returns an error.

    Example:
        >>> result = SkyQuery.uploadTable("Column1,Column2\n4.5,5.5\n", tableName="myTable", datasetName="MyDB", informat="csv")

    See Also:
        SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo,
        SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.listTableColumns,
        SkyQuery.getTable

    """

    url = '{0}/Data.svc/{1}/{2}'.format(config.SkyQueryUrl, datasetName, tableName)
    ctype = ""
    if informat == "csv":
        ctype = 'text/csv'
    else:
        raise Exception("Unknown format {0} when trying to upload data in SkyQuery.".format(informat))

    response = send_request(url, reqtype='put', data=uploadData, content_type=ctype, stream=True,
                            acceptHeader='application/json',
                            errmsg='Error when uploading data to table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        return True


