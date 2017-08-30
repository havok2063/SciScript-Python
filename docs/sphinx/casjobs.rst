
.. _sciserver-casjobs:

CasJobs
=======

CasJobs is an online workbench for large scientific catalogs, designed to emulate and enhance local free-form query access in a web environment.  It is designed as an interface into the CAS system.  CAS stands for Catalog Archive Server, an online repository for catalog and metadata information of your datasets.

Some features of this application include:

* Both synchronous and asynchronous query execution, in the form of 'quick' and 'long' jobs.
* A query 'History' that records queries and their status.
* A server-side, personalized user database, called 'MyDB', enabling persistant table/function/procedure creation.
* Data sharing between users, via the 'Groups' mechanism.
* Data download, via MyDB table extraction, in various formats.
* Multiple interface options, including a browser client as well as a java-based command line tool.

Casjobs includes an API to allow for programmatic access your catalog data.  Check out the :download:`CasJobs REST API<CasJobs_REST_API.pdf>` for a full description of the routes available.

Getting Started
^^^^^^^^^^^^^^^

.. _sciserver_casjobs_api:

Reference/API
^^^^^^^^^^^^^

See the :ref:`sciserver-ref-casjobs` reference section for all details on the Python wrapper tools.

.. rubric:: Class

.. autosummary:: sciserver.casjobs.CasJobs

.. rubric:: Methods

.. autosummary::

    sciserver.casjobs.CasJobs.executeQuery
    sciserver.casjobs.CasJobs.submitJob
    sciserver.casjobs.CasJobs.waitForJob
    sciserver.casjobs.CasJobs.getJobStatus
    sciserver.casjobs.CasJobs.cancelJob
    sciserver.casjobs.CasJobs.writeFitsFileFromQuery
    sciserver.casjobs.CasJobs.getPandasDataFrameFromQuery
    sciserver.casjobs.CasJobs.getNumpyArrayFromQuery
    sciserver.casjobs.CasJobs.getTables
    sciserver.casjobs.CasJobs.uploadPandasDataFrameToTable
    sciserver.casjobs.CasJobs.uploadCSVDataToTable


