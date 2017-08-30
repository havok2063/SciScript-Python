# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:56:07
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 14:50:33

from __future__ import print_function, division, absolute_import
from io import StringIO, BytesIO
import json
import time
import requests as requests
import pandas
import os
from sciserver import config
from sciserver.authentication import Authentication
from sciserver.utils import checkAuth, send_request, Task


class CasJobs(object):
    ''' This class contains methods for interacting with CasJobs '''

    def __init__(self):
        self.baseURI = config.CasJobsRESTUri
        self.contextURI = self.make_uri('contexts')

    def make_uri(self, path, base=None):
        base = base if base else self.baseURI
        uri = os.path.join(base, path)
        return uri

    def get_taskname(self, name):
        task = Task(name, use_base=True, component='CasJobs')
        return task.name

    @checkAuth
    def getSchemaName(self):
        """ Returns an id for a database schema

        Returns the WebServiceID that identifies the schema for a
        user in MyScratch database with CasJobs.

        Returns:
            id (str):
                the WebServerID of the user

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> wsid = CasJobs.getSchemaName()

        See Also:
            CasJobs.getTables.

        """

        auth = Authentication(token=config.token)
        keystoneUserId = auth.getKeystoneUserWithToken().userid

        usersUrl = self.make_uri(os.path.join('users', keystoneUserId))

        response = send_request(usersUrl, content_type='application/json',
                                errmsg='Error when getting schema name')
        if response.ok:
            jsonResponse = json.loads(response.content.decode())
            return "wsid_" + str(jsonResponse["WebServicesId"])

    @checkAuth
    def getTables(self, context="MyDB"):
        """ Gets the info for all tables in a db

        Gets the names, size and creation date of all tables in a
        database context that the user has access to.

        Parameters:
            context (str):
                the database context string (i.e. name of the db)

        Returns:
            a JSON object with format [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> tables = CasJobs.getTables("MyDB")

        See Also:
            CasJobs.getSchemaName.

        """

        tablesUrl = self.make_uri(os.path.join(context, 'Tables'), base=self.contextURI)

        response = send_request(tablesUrl, content_type='application/json',
                                errmsg='Error when getting table description from database')

        if response.ok:
            jsonResponse = json.loads(response.content.decode())
            return jsonResponse

    def executeQuery(self, sql, context="MyDB", outformat="pandas"):
        """
        Executes a synchronous SQL query in a CasJobs database context.

        Parameters:
            sql (str):
                the sql query string
            context (str):
                the database context string (i.e. name of the db)
            outformat (str):
                the format of return output type. Default is Pandas dataframe.
                Options are:
                \t\t'pandas': pandas.DataFrame.\n
                \t\t'json': a JSON string containing the query results. \n
                \t\t'dict': a dictionary created from the JSON string containing the query results.\n
                \t\t'csv': a csv string.\n
                \t\t'readable': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
                \t\t'StringIO': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
                \t\t'fits': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n
                \t\t'BytesIO': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n

        Returns:
            The query result table, in the format specified

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> table = CasJobs.executeQuery(sql="select 1 as foo, 2 as bar",format="pandas", context="MyDB")

        See Also:
            CasJobs.submitJob, CasJobs.getTables, SkyServer.sqlSearch

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

        QueryUrl = self.make_uri(os.path.join(context, 'query'), base=self.contextURI)

        TaskName = self.get_taskname('executeQuery')

        query = {"Query": sql, "TaskName": TaskName}

        data = json.dumps(query).encode()

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
    def submitJob(self, sql, context="MyDB"):
        """
        Submits an asynchronous SQL query to the CasJobs queue.

        Parameters:
            sql (str):
                the sql query string
            context (str):
                the database context string (i.e. name of the db)

        Returns:
            The CasJobs job id

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> jobid = CasJobs.submitJob("select 1 as foo","MyDB")

        See Also:
            CasJobs.executeQuery, CasJobs.getJobStatus, CasJobs.waitForJob, CasJobs.cancelJob.

        """

        QueryUrl = self.make_uri(os.path.join(context, 'jobs'), base=self.contextURI)

        TaskName = self.get_taskname('submitJob')

        query = {"Query": sql, "TaskName": TaskName}

        data = json.dumps(query).encode()

        response = send_request(QueryUrl, reqtype='put', data=data,
                                content_type='application/json', acceptHeader='text/plain',
                                errmsg='Error when getting schema name')

        if response.ok:
            return int(response.content.decode())

    @checkAuth
    def getJobStatus(self, jobId):
        """ Get a job status

        Shows the status of a job submitted to CasJobs.

        Parameters:
            jobId (int):
                the id of the submitted job

        Returns:
            a dictionary containing the job status and related metadata.

            The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling),
            3(Canceled), 4 (Failed) or 5 (Finished). If jobId is the empty string, then returns a list
            with the statuses of all previous jobs.

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> status = CasJobs.getJobStatus(CasJobs.submitJob("select 1"))

        See Also:
            CasJobs.submitJob, CasJobs.waitForJob, CasJobs.cancelJob.

        """

        QueryUrl = self.make_uri(os.path.join('jobs', str(jobId)))

        response = send_request(QueryUrl, content_type='application/json',
                                errmsg='Error when getting the status of job {0}'.format(jobId))
        if response.ok:
            return json.loads(response.content.decode())

    @checkAuth
    def cancelJob(self, jobId):
        """
        Cancels a job already submitted.

        Parameters:
            jobId (int):
                the id of the submitted job

        Returns:
            True if the job was canceled successfully

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> response = CasJobs.cancelJob(CasJobs.submitJob("select 1"))

        See Also:
            CasJobs.submitJob, CasJobs.waitForJob

        """

        QueryUrl = self.make_uri(os.path.join('jobs', str(jobId)))

        response = send_request(QueryUrl, reqtype='delete', content_type='application/json',
                                errmsg='Error when canceling job {0}'.format(jobId))
        if response.ok:
            return True  # json.loads(response.content)

    def waitForJob(self, jobId, verbose=True):
        """ Waits for a job to finish

        Queries the job status from casjobs every 2 seconds and waits for the
        casjobs job to return a status of 3, 4, or 5 (Cancelled, Failed or
        Finished, respectively).

        Parameters:
            jobId (int):
                the id of the submitted job
            verbose (bool):
                If True, prints 'wait' messages to the screen

        Returns:
            a dictonary containing the job status and related metadata

            The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling), 3(Canceled), 4 (Failed) or 5 (Finished).

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> CasJobs.waitForJob(CasJobs.submitJob("select 1"))

        See Also:
            CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.cancelJob

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
                jobDesc = self.getJobStatus(jobId)
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

    def writeFitsFileFromQuery(self, fileName, queryString, context="MyDB"):
        """ Performs a quick query and writes a FITS file

        Executes a quick CasJobs query and writes the result to a local Fits file
        (http://www.stsci.edu/institute/software_hardware/pyfits).

        Parameters:
            fileName (str):
                path to the local Fits file to be created
            queryString (str):
                the sql query string
            context (str):
                the name of the db

        Returns:
            True if the FITS file was created successfully

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> CasJobs.writeFitsFileFromQuery("/home/user/myFile.fits","select 1 as foo")

        See Also:
            CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
            CasJobs.getPandasDataFrameFromQuery, CasJobs.getNumpyArrayFromQuery

        """
        try:
            bytesio = self.executeQuery(queryString, context=context, outformat="fits")

            theFile = open(fileName, "w+b")
            theFile.write(bytesio.read())
            theFile.close()

            return True

        except Exception as e:
            raise e

    def getPandasDataFrameFromQuery(self, queryString, context="MyDB"):
        """ Performs a quick query and outputs a Pandas dataframe

        Executes a casjobs quick query and returns the result as a
        pandas dataframe object with an index
        (http://pandas.pydata.org/pandas-docs/stable/).

        Parameters:
            queryString (str):
                the sql query string
            context (str):
                the name of the db

        Returns:
            a Pandas dataframe containing the results table

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> df = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB")

        See Also:
            CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
            CasJobs.writeFitsFileFromQuery, CasJobs.getNumpyArrayFromQuery

        """
        try:
            cvsResponse = self.executeQuery(queryString, context=context, outformat="readable")

            # if the index column is not specified then it will add it's own column which causes
            # problems when uploading the transformed data
            dataFrame = pandas.read_csv(cvsResponse, index_col=None)

            return dataFrame

        except Exception as e:
            raise e

    def getNumpyArrayFromQuery(self, queryString, context="MyDB"):
        """ Performs a quick query and outputs a numpy array

        Executes a casjobs query and returns the results table as a Numpy array
        (http://docs.scipy.org/doc/numpy/).

        Parameters:
            queryString (str):
                the sql query string
            context (str):
                the name of the db

        Returns:
            a Numpy array storing the results table

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> array = CasJobs.getNumpyArrayFromQuery("select 1 as foo", context="MyDB")

        See Also:
            CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery,
            CasJobs.writeFitsFileFromQuery, CasJobs.getPandasDataFrameFromQuery

        """
        try:

            dataFrame = self.getPandasDataFrameFromQuery(queryString, context)
            return dataFrame.as_matrix()

        except Exception as e:
            raise e

    def uploadPandasDataFrameToTable(self, dataFrame, tableName, context="MyDB"):
        """ Upload a Pandas dataframe

        Uploads a pandas dataframe object into a CasJobs table.
        If the dataframe contains a named index, then the index will be uploaded as
        a column as well.

        Parameters:
            dataFrame:
                Pandas data frame containg the data (pandas.core.frame.DataFrame)
            tableName (str):
                the name of the CasJobs table to be created
            context (str):
                the name of the db

        Returns:
            True if the dataframe was uploaded successfully

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> response = CasJobs.uploadPandasDataFrameToTable(CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB"), "NewTableFromDataFrame")

        See Also:
            CasJobs.uploadCSVDataToTable

        """
        try:
            if dataFrame.index.name is not None and dataFrame.index.name != "":
                sio = dataFrame.to_csv().encode("utf8")
            else:
                sio = dataFrame.to_csv(index_label=False, index=False).encode("utf8")

            return self.uploadCSVDataToTable(sio, tableName, context)

        except Exception as e:
            raise e

    @checkAuth
    def uploadCSVDataToTable(self, csvData, tableName, context="MyDB"):
        """
        Uploads CSV data into a CasJobs table.

        Parameters:
            csvData:
                a CSV table in string format.
            tableName (str):
                the name of the CasJobs table to be created
            context (str):
                the name of the db

        Returns:
            True if the csv data was uploaded successfully

        Raises:
            Throws an exception if the HTTP request to the CasJobs API returns an error.

        Example:
            >>> csv = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB").to_csv().encode("utf8"); response = CasJobs.uploadCSVDataToTable(csv, "NewTableFromDataFrame")

        See Also:
            CasJobs.uploadPandasDataFrameToTable

        """

        tablesUrl = self.make_uri(os.path.join(context, 'Tables', tableName), base=self.contextURI)

        postResponse = send_request(tablesUrl, reqtype='post', data=csvData, stream=True,
                                    content_type='application/json',
                                    errmsg='Error when uploading CSV data into CasJobs table {0}'.format(tableName))
        if postResponse.ok:
            return True

