# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:56:07
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-07 14:09:47

from __future__ import print_function, division, absolute_import
from io import StringIO, BytesIO
import json
import time
import requests as requests
import pandas
from sciserver import authentication, config
from sciserver.utils import checkAuth, send_request


@checkAuth
def getSchemaName():
    """
    Returns the WebServiceID that identifies the schema for a user in MyScratch database with CasJobs.

    :return: WebServiceID of the user (string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: wsid = CasJobs.getSchemaName()

    .. seealso:: CasJobs.getTables.
    """
    #token = authentication.getToken()
    # if token is not None and token != "":

    keystoneUserId = authentication.getKeystoneUserWithToken(config.token).id
    usersUrl = config.CasJobsRESTUri + "/users/" + keystoneUserId
    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}
    # getResponse = requests.get(usersUrl, headers=headers)
    # if getResponse.status_code != 200:
    #     raise Exception("Error when getting schema name. Http Response from CasJobs API "
    #                     "returned status code {0}: \n{1}".format(getResponse.status_code, getResponse.content.decode()))

    response = send_request(usersUrl, content_type='application/json',
                            errmsg='Error when getting schema name')
    if response.ok:
        jsonResponse = json.loads(response.content.decode())
        return "wsid_" + str(jsonResponse["WebServicesId"])
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def getTables(context="MyDB"):
    """
    Gets the names, size and creation date of all tables in a database context that the user
    has access to.

    :param context: database context (string)
    :return: The result is a json object with format [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: tables = CasJobs.getTables("MyDB")

    .. seealso:: CasJobs.getSchemaName
    """

    #token = authentication.getToken()
    # if token is not None and token != "":

    TablesUrl = config.CasJobsRESTUri + "/contexts/" + context + "/Tables"

    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}

    # getResponse = requests.get(TablesUrl, headers=headers)

    # if getResponse.status_code != 200:
    #     raise Exception("Error when getting table description from database "
    #                     "context {0}. \nHttp Response from CasJobs API returned status "
    #                     "code {1}: \n{2}".format(context, getResponse.status_code, getResponse.content.decode()))

    response = send_request(TablesUrl, content_type='application/json',
                            errmsg='Error when getting table description from database')

    if response.ok:
        jsonResponse = json.loads(response.content.decode())
        return jsonResponse
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


def executeQuery(sql, context="MyDB", outformat="pandas"):
    """
    Executes a synchronous SQL query in a CasJobs database context.

    :param sql: sql query (string)
    :param context: database context (string)
    :param format: parameter (string) that specifies the return type:\n
    \t\t'pandas': pandas.DataFrame.\n
    \t\t'json': a JSON string containing the query results. \n
    \t\t'dict': a dictionary created from the JSON string containing the query results.\n
    \t\t'csv': a csv string.\n
    \t\t'readable': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
    \t\t'StringIO': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
    \t\t'fits': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n
    \t\t'BytesIO': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n
    :return: the query result table, in a format defined by the 'format' input parameter.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error. Throws an exception if parameter 'format' is not correctly specified.
    :example: table = CasJobs.executeQuery(sql="select 1 as foo, 2 as bar",format="pandas", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getTables, SkyServer.sqlSearch
    """

    if (outformat == "pandas") or (outformat == "json") or (outformat == "dict"):
        acceptHeader = "application/json+array"
    elif (outformat == "csv") or (outformat == "readable") or (outformat == "StringIO"):
        acceptHeader = "text/plain"
    elif outformat == "fits":
        acceptHeader = "application/fits"
    elif outformat == "BytesIO":
        acceptHeader = "application/fits"  # defined later using specific serialization
    else:
        raise Exception("Error when executing query. Illegal format parameter specification: {0}".format(outformat))

    QueryUrl = config.CasJobsRESTUri + "/contexts/" + context + "/query"

    TaskName = ""
    if config.isSciServerComputeEnvironment():
        TaskName = "Compute.SciScript-Python.CasJobs.executeQuery"
    else:
        TaskName = "SciScript-Python.CasJobs.executeQuery"

    query = {"Query": sql, "TaskName": TaskName}

    data = json.dumps(query).encode()

    # headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}
    # token = authentication.getToken()
    # if token is not None and token != "":
    #     headers['X-Auth-Token'] = token

    # postResponse = requests.post(QueryUrl, data=data, headers=headers, stream=True)
    # if postResponse.status_code != 200:
    #     raise Exception("Error when executing query. Http Response from CasJobs API "
    #                     "returned status code {0}: \n{1}".format(postResponse.status_code, postResponse.content.decode()))

    postResponse = send_request(QueryUrl, reqtype='post', data=data, stream=True,
                                content_type='application/json', acceptHeader=acceptHeader,
                                errmsg='Error when getting schema name')

    if postResponse.ok:
        if (outformat == "readable") or (outformat == "StringIO"):
            return StringIO(postResponse.content.decode())
        elif outformat == "pandas":
            r = json.loads(postResponse.content.decode())
            return pandas.DataFrame(r['Result'][0]['Data'], columns=r['Result'][0]['Columns'])
        elif outformat == "csv":
            return postResponse.content.decode()
        elif outformat == "dict":
            return json.loads(postResponse.content.decode())
        elif outformat == "json":
            return postResponse.content.decode()
        elif outformat == "fits":
            return BytesIO(postResponse.content)
        elif outformat == "BytesIO":
            return BytesIO(postResponse.content)
        else:  # should not occur
            raise Exception("Error when executing query. Illegal format parameter specification: {0}".format(outformat))


@checkAuth
def submitJob(sql, context="MyDB"):
    """
    Submits an asynchronous SQL query to the CasJobs queue.

    :param sql: sql query (string)
    :param context: database context (string)
    :return: Returns the CasJobs jobID (integer).
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: jobid = CasJobs.submitJob("select 1 as foo","MyDB")

    .. seealso:: CasJobs.executeQuery, CasJobs.getJobStatus, CasJobs.waitForJob, CasJobs.cancelJob.
    """
    # token = authentication.getToken()
    # if token is not None and token != "":

    QueryUrl = config.CasJobsRESTUri + "/contexts/" + context + "/jobs"

    TaskName = ""
    if config.isSciServerComputeEnvironment():
        TaskName = "Compute.SciScript-Python.CasJobs.submitJob"
    else:
        TaskName = "SciScript-Python.CasJobs.submitJob"

    query = {"Query": sql, "TaskName": TaskName}

    data = json.dumps(query).encode()

    # headers = {'Content-Type': 'application/json', 'Accept': "text/plain"}
    # headers['X-Auth-Token'] = token

    # putResponse = requests.put(QueryUrl, data=data, headers=headers)
    # if putResponse.status_code != 200:
    #     raise Exception("Error when submitting a job. Http Response from CasJobs API "
    #                     "returned status code {0}:\n {1}".format(putResponse.status_code, putResponse.content.decode()))

    response = send_request(QueryUrl, reqtype='put', data=data,
                            content_type='application/json', acceptHeader='text/plain',
                            errmsg='Error when getting schema name')

    if response.ok:
        return int(response.content.decode())
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def getJobStatus(jobId):
    """
    Shows the status of a job submitted to CasJobs.

    :param jobId: id of job (integer)
    :return: Returns a dictionary object containing the job status and related metadata.
    The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling), 3(Canceled), 4 (Failed) or 5 (Finished).
    If jobId is the empty string, then returns a list with the statuses of all previous jobs.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login for that purpose).
    Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: status = CasJobs.getJobStatus(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob, CasJobs.cancelJob.
    """
    # token = authentication.getToken()
    # if token is not None and token != "":

    QueryUrl = config.CasJobsRESTUri + "/jobs/" + str(jobId)

    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}

    # postResponse = requests.get(QueryUrl, headers=headers)
    # if postResponse.status_code != 200:
    #     raise Exception("Error when getting the status of job {0}. "
    #                     "Http Response from CasJobs API returned status"
    #                     "code {1}:\n {2}".format(jobId, postResponse.status_code, postResponse.content.decode()))

    response = send_request(QueryUrl, content_type='application/json',
                            errmsg='Error when getting the status of job {0}'.format(jobId))
    if response.ok:
        return json.loads(response.content.decode())
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


@checkAuth
def cancelJob(jobId):
    """
    Cancels a job already submitted.

    :param jobId: id of job (integer)
    :return: Returns True if the job was canceled successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: response = CasJobs.cancelJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob.
    """
    # token = authentication.getToken()
    # if token is not None and token != "":

    QueryUrl = config.CasJobsRESTUri + "/jobs/" + str(jobId)

    # headers = {'X-Auth-Token': token, 'Content-Type': 'application/json'}

    # response = requests.delete(QueryUrl, headers=headers)
    # if response.status_code != 200:
    #     raise Exception("Error when canceling job {0}. "
    #                     "Http Response from CasJobs API returned status code {1}:"
    #                     "\n {2}".format(jobId, response.status_code, response.content.decode()))

    response = send_request(QueryUrl, reqtype='delete', content_type='application/json',
                            errmsg='Error when canceling job {0}'.format(jobId))
    if response.ok:
        return True  # json.loads(response.content)
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")


def waitForJob(jobId, verbose=True):
    """
    Queries the job status from casjobs every 2 seconds and waits for the casjobs job to return a
    status of 3, 4, or 5 (Cancelled, Failed or Finished, respectively).

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the job is not done.
    If False, will suppress printing messages on the screen.
    :return: After the job is finished, returns a dictionary object containing the job status
    and related metadata. The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling),
    3(Canceled), 4 (Failed) or 5 (Finished).
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: CasJobs.waitForJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.cancelJob.
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
            jobStatus = int(jobDesc["Status"])
            if jobStatus in (3, 4, 5):
                complete = True
                if verbose:
                    print("Done!")
            else:
                time.sleep(2)

        return jobDesc
    except Exception as e:
        raise e


def writeFitsFileFromQuery(fileName, queryString, context="MyDB"):
    """
    Executes a quick CasJobs query and writes the result to a local Fits file
    (http://www.stsci.edu/institute/software_hardware/pyfits).

    :param fileName: path to the local Fits file to be created (string)
    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns True if the fits file was created successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: CasJobs.writeFitsFileFromQuery("/home/user/myFile.fits","select 1 as foo")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.getPandasDataFrameFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:
        bytesio = executeQuery(queryString, context=context, outformat="fits")

        theFile = open(fileName, "w+b")
        theFile.write(bytesio.read())
        theFile.close()

        return True

    except Exception as e:
        raise e


# no explicit index column by default
def getPandasDataFrameFromQuery(queryString, context="MyDB"):
    """
    Executes a casjobs quick query and returns the result as a pandas dataframe object with
    an index (http://pandas.pydata.org/pandas-docs/stable/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Pandas dataframe containing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: df = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.writeFitsFileFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:
        cvsResponse = executeQuery(queryString, context=context, outformat="readable")

        # if the index column is not specified then it will add it's own column which causes
        # problems when uploading the transformed data
        dataFrame = pandas.read_csv(cvsResponse, index_col=None)

        return dataFrame

    except Exception as e:
        raise e


def getNumpyArrayFromQuery(queryString, context="MyDB"):
    """
    Executes a casjobs query and returns the results table as a Numpy array
    (http://docs.scipy.org/doc/numpy/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Numpy array storing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: array = CasJobs.getNumpyArrayFromQuery("select 1 as foo", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.writeFitsFileFromQuery, CasJobs.getPandasDataFrameFromQuery

    """
    try:

        dataFrame = getPandasDataFrameFromQuery(queryString, context)
        return dataFrame.as_matrix()

    except Exception as e:
        raise e


# require pandas for now but be able to take a string in the future
def uploadPandasDataFrameToTable(dataFrame, tableName, context="MyDB"):
    """
    Uploads a pandas dataframe object into a CasJobs table. If the dataframe contains a named index,
    then the index will be uploaded as a column as well.

    :param dataFrame: Pandas data frame containg the data (pandas.core.frame.DataFrame)
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the dataframe was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: response = CasJobs.uploadPandasDataFrameToTable(CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB"), "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadCSVDataToTable
    """
    try:
        if dataFrame.index.name is not None and dataFrame.index.name != "":
            sio = dataFrame.to_csv().encode("utf8")
        else:
            sio = dataFrame.to_csv(index_label=False, index=False).encode("utf8")

        return uploadCSVDataToTable(sio, tableName, context)

    except Exception as e:
        raise e


@checkAuth
def uploadCSVDataToTable(csvData, tableName, context="MyDB"):
    """
    Uploads CSV data into a CasJobs table.

    :param csvData: a CSV table in string format.
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the csv data was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login
    for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: csv = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB").to_csv().encode("utf8"); response = CasJobs.uploadCSVDataToTable(csv, "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadPandasDataFrameToTable
    """
    # token = authentication.getToken()
    # if token is not None and token != "":

    # if (config.executeMode == "debug"):
    #    print("Uploading ", sys.getsizeof(CVSdata), "bytes...")
    tablesUrl = config.CasJobsRESTUri + "/contexts/" + context + "/Tables/" + tableName

    # headers = {}
    # headers['X-Auth-Token'] = token

    # postResponse = requests.post(tablesUrl, data=csvData, headers=headers, stream=True)
    # if postResponse.status_code != 200:
    #     raise Exception("Error when uploading CSV data into CasJobs table {0}. "
    #                     "Http Response from CasJobs API returned status code {1}:"
    #                     "\n {2}".format(tableName, postResponse.status_code, postResponse.content.decode()))

    postResponse = send_request(tablesUrl, reqtype='post', data=csvData, stream=True,
                                content_type='application/json',
                                errmsg='Error when uploading CSV data into CasJobs table {0}'.format(tableName))
    if postResponse.ok:
        return True
    # else:
    #     raise Exception("User token is not defined. First log into SciServer.")
