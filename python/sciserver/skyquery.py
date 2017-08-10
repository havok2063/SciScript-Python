# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 16:00:25
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 10:28:32

from __future__ import print_function, division, absolute_import
from io import StringIO
import json
import time
import pandas
from sciserver import config
from sciserver.utils import checkAuth, send_request


@checkAuth
def getJobStatus(jobId):
    """
    Gets the status of a job, as well as other related metadata (more info in http://www.voservices.net/skyquery).

    :param jobId: the ID of the job (string), which is obtained at the moment of submitting the job.
    :return: a dictionary with the job status and other related metadata.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: status = SkyQuery.getJobStatus(SkyQuery.submitJob("select 1 as foo"))

    .. seealso:: SkyQuery.submitJob, SkyQuery.cancelJob
    """

    statusURL = '{0}/Jobs.svc/jobs/{1}'.format(config.SkyQueryUrl, jobId)

    response = send_request(statusURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when getting job status {0}'.format(jobId))
    if response.ok:
        r = response.json()
        return r['queryJob']


@checkAuth
def cancelJob(jobId):
    """
    Cancels a single job (more info in http://www.voservices.net/skyquery).

    :param jobId: the ID of the job, which is obtained at the moment of submitting the job.
    :return: Returns True if the job was cancelled successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: isCanceled = SkyQuery.cancelJob(SkyQuery.submitJob("select 1 as foo"))

    .. seealso:: SkyQuery.submitJob, SkyQuery.getJobStatus
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
    """
    Returns a list of all available job queues and related metadata (more info in http://www.voservices.net/skyquery).

    :return: a list of all available job queues and related metadata.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: queueList = SkyQuery.listQueues()

    .. seealso:: SkyQuery.getQueueInfo, SkyQuery.submitJob, SkyQuery.getJobStatus
    """

    jobsURL = '{0}/Jobs.svc/queues'.format(config.SkyQueryUrl)

    response = send_request(jobsURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when listing queues')
    if response.ok:
        r = response.json()
        return r['queues']


@checkAuth
def getQueueInfo(queue):
    """
    Returns information about a particular job queue (more info in http://www.voservices.net/skyquery).

    :param queue: queue name (string)
    :return: a dictionary containing information associated to the queue.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: queueInfo = SkyQuery.getQueueInfo('quick')

    .. seealso:: SkyQuery.listQueues, SkyQuery.submitJob, SkyQuery.getJobStatus
    """

    jobsURL = '{0}/Jobs.svc/queues/{1}'.format(config.SkyQueryUrl, queue)

    response = send_request(jobsURL, content_type='application/json', acceptHeader='application/json',
                            errmsg='Error when getting queue info {0}'.format(queue))

    if response.ok:
        r = response.json()
        return r['queue']


@checkAuth
def submitJob(query, queue='quick'):
    """
    Submits a new job (more info in http://www.voservices.net/skyquery).

    :param query: sql query (string)
    :param queue: queue name (string). Can be set to 'quick' for a quick job, or 'long' for a long job.
    :return: returns the jobId (string), unique identifier of the job.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: jobId = SkyQuery.submitJob('select 1 as foo', "quick")

    .. seealso:: SkyQuery.getJobStatus, SkyQuery.listQueues
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
    """
    Queries the job status from SkyQuery every 2 seconds and waits for the SkyQuery job to be completed.

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the job is not done. If False, will suppress printing messages on the screen.
    :return: After the job is finished, returns a dictionary object containing the job status and related metadata.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: SkyQuery.waitForJob(SkyQuery.submitJob("select 1"))

    .. seealso:: SkyQuery.submitJob, SkyQuery.getJobStatus.
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
    """
    Lists the jobs in the queue in descending order by submission time. Only jobs of the authenticated user are listed (more info in http://www.voservices.net/skyquery).

    :param queue: queue name (string). Can be set to 'quick' for a quick job, or 'long' for a long job.
    :return: returns job definitions as a list object.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: jobsList = SkyQuery.listJobs('quick')

    .. seealso:: SkyQuery.getJobStatus, SkyQuery.listQueues
    """

    jobsURL = '{0}/Jobs.svc/queues/{1}/jobs?'.format(config.SkyQueryUrl, queue)

    response = send_request(jobsURL, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when listing jobs on queue {0}'.format(queue))
    if response.ok:
        r = response.json()
        return r['jobs']


# Schema:


@checkAuth
def listAllDatasets():
    """
    Lists all available datasets (more info in http://www.voservices.net/skyquery).

    :return: returns dataset definitions as a list object.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: datasets = SkyQuery.listAllDatasets()

    .. seealso:: SkyQuery.listQueues, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable, SkyQuery.dropTable
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
    Gets information related to a particular dataset (more info in http://www.voservices.net/skyquery).

    :param datasetName: name of dataset (string).
    :return: returns a dictionary containing the dataset information.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: info = SkyQuery.getDatasetInfo("MyDB")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable, SkyQuery.dropTable
    """

    schemaURL = '{0}/Schema.svc/datasets/{1}'.format(config.SkyQueryUrl, datasetName)

    response = send_request(schemaURL, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when getting info from dataset {0}'.format(datasetName))
    if response.ok:
        r = response.json()
        return r


def listDatasetTables(datasetName="MyDB"):
    """
    Returns a list of all tables within a dataset (more info in http://www.voservices.net/skyquery).

    :param datasetName: name of dataset (string).
    :return: returns a list containing the tables and associated descriptions/metadata.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: tables = SkyQuery.listDatasetTables("MyDB")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.getTableInfo, SkyQuery.listTableColumns, SkyQuery.getTable, SkyQuery.dropTable
    """

    url = '{0}/Schema.svc/datasets/{1}/tables'.format(config.SkyQueryUrl, datasetName)

    response = send_request(url, content_type='application/json',
                            acceptHeader='application/json', errmsg='Error when listing tables in dataset {0}'.format(datasetName))
    if response.ok:
        r = response.json()
        return r['tables']


@checkAuth
def getTableInfo(tableName, datasetName="MyDB"):
    """
    Returns info about a particular table belonging to a dataset (more info in http://www.voservices.net/skyquery).

    :param tableName: name of table (string) within dataset.
    :param datasetName: name of dataset (string).
    :return: returns a dictionary containing the table properties and associated info/metadata.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: info = SkyQuery.getTableInfo("myTable", datasetName="MyDB")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.listTableColumns, SkyQuery.getTable, SkyQuery.dropTable
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
    """
    Returns a list of all columns in a table belonging to a particular dataset (more info in http://www.voservices.net/skyquery).

    :param tableName: name of table (string) within dataset.
    :param datasetName: name of dataset (string).
    :return: returns a list containing the columns and associated descriptions.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: columns = SkyQuery.listTableColumns("myTable", datasetName="MyDB")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.getTable, SkyQuery.dropTable
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
    """
    Returns a dataset table as a pandas DataFrame (more info in http://www.voservices.net/skyquery).

    :param tableName: name of table (string) within dataset.
    :param datasetName: name of dataset or database context (string).
    :param top: number of top rows retrieved (integer).
    :return: returns the table as a Pandas dataframe.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: table = SkyQuery.getTable("myTable", datasetName="MyDB", top=10)

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.dropTable, SkyQuery.submitJob
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
    """
    Drops (deletes) a table from the user database (more info in http://www.voservices.net/skyquery).

    :param tableName: name of table (string) within dataset.
    :param datasetName: name of dataset or database context (string).
    :return: returns True if the table was deleted successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: result = SkyQuery.dropTable("myTable", datasetName="MyDB")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.getTable, SkyQuery.submitJob
    """

    url = '{0}/Data.svc/{1}/{2}'.format(config.SkyQueryUrl, datasetName, tableName)

    response = send_request(url, reqtype='delete', content_type='application/json',
                            acceptHeader='application/json',
                            errmsg='Error when dropping table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        return True


@checkAuth
def uploadTable(uploadData, tableName, datasetName="MyDB", outformat="csv"):
    """
    Uploads a data table into a database (more info in http://www.voservices.net/skyquery).

    :param uploadData: data table, for now accepted in CSV string format.
    :param tableName: name of table (string) within dataset.
    :param datasetName: name of dataset or database context (string).
    :param format: format of the 'data' parameter. Set to 'csv' for now.
    :return: returns True if the table was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the SkyQuery API returns an error.
    :example: result = SkyQuery.uploadTable("Column1,Column2\n4.5,5.5\n", tableName="myTable", datasetName="MyDB", format="csv")

    .. seealso:: SkyQuery.listQueues, SkyQuery.listAllDatasets, SkyQuery.getDatasetInfo, SkyQuery.listDatasetTables, SkyQuery.getTableInfo, SkyQuery.getTable, SkyQuery.submitJob
    """

    url = '{0}/Data.svc/{1}/{2}'.format(config.SkyQueryUrl, datasetName, tableName)
    ctype = ""
    if outformat == "csv":
        ctype = 'text/csv'
    else:
        raise Exception("Unknown format {0} when trying to upload data in SkyQuery.".format(outformat))

    response = send_request(url, reqtype='put', data=uploadData, content_type=ctype, stream=True,
                            acceptHeader='application/json',
                            errmsg='Error when uploading data to table {0} in dataset {1}'.format(tableName, datasetName))
    if response.ok:
        return True


