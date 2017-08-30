.. _sciserver-skyquery:

SkyQuery
========

SkyQuery has been developed over the past few years to support scalable access to large astronomy databases, and in particular to provide distributed query capability to support “statistical cross-match” queries across a federation of data sources.  Develop in C# and ASP.NET, SkyQuery includes a number of technologies that will be extended to provide the basis for SciServer scalability – a GrayWulf Cluster (see below for more information) for data storage, distributed query execution, workflow management, authentication, and a Web Serviced API for integration and interoperability.

Getting Started
^^^^^^^^^^^^^^^

.. _sciserver_skyquery_api:

Reference/API
^^^^^^^^^^^^^

See the :ref:`sciserver-ref-skyquery` reference section for all details on the Python wrapper tools.

.. rubric:: Class

.. autosummary:: sciserver.skyquery.SkyQuery

.. rubric:: Methods

.. autosummary::

    sciserver.skyquery.SkyQuery.uploadTable
    sciserver.skyquery.SkyQuery.dropTable
    sciserver.skyquery.SkyQuery.getTable
    sciserver.skyquery.SkyQuery.getTableInfo
    sciserver.skyquery.SkyQuery.listTableColumns
    sciserver.skyquery.SkyQuery.listDatasetTables
    sciserver.skyquery.SkyQuery.getDatasetInfo
    sciserver.skyquery.SkyQuery.listAllDatasets
    sciserver.skyquery.SkyQuery.listJobs
    sciserver.skyquery.SkyQuery.waitForJob
    sciserver.skyquery.SkyQuery.submitJob
    sciserver.skyquery.SkyQuery.cancelJob
    sciserver.skyquery.SkyQuery.getJobStatus
    sciserver.skyquery.SkyQuery.listQueues
    sciserver.skyquery.SkyQuery.getQueueInfo
