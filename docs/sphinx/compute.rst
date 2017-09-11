.. _sciserver-compute:

Compute
========

Compute optimizes Big Data science by allowing users to bring their analysis close to the data with Jupyter Notebooks deployed in server-side containers.

SciServer Compute uses Jupyter Notebooks running within server-side containers attached to big data collections to bring advanced analysis to big data “in the cloud.” Compute extends the popular CasJobs and SkyServer systems with server-side computational capabilities and very large scratch storage space, and further extends their functions to a range of other scientific disciplines.

Getting Started
^^^^^^^^^^^^^^^

.. _sciserver_compute_api:

Reference/API
^^^^^^^^^^^^^

See the :ref:`sciserver-ref-compute` reference section for all details on the Python wrapper tools.

.. rubric:: Class

.. autosummary:: sciserver.compute.Compute
.. autosummary:: sciserver.compute.Job

.. rubric:: Methods

.. autosummary::

    sciserver.compute.Compute.retrieveDomains
    sciserver.compute.Compute.getJobs
    sciserver.compute.Compute.getJob
    sciserver.compute.Compute.getJobStatus
    sciserver.compute.Compute.isJobFinished
    sciserver.compute.Compute.submitQuery
    sciserver.compute.Compute.waitFor
    sciserver.compute.Job.check_error
    sciserver.compute.Job.is_finished
    sciserver.compute.Job.upload
    sciserver.compute.Job.retrieveData
    sciserver.compute.Job.loadDataFrame

