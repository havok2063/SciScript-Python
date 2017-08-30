.. _sciserver-scidrive:

SciDrive
========

SciDrive evolved from the Virtual Observatory (VO) project, to provide a Dropbox-like interface for scientific data, with a mechanism for data and metadata extraction (under development), and that can interface directly with CasJobs, providing integrated access to CasJob databases and MyDB.


Getting Started
^^^^^^^^^^^^^^^

.. _sciserver_scidrive_api:

Reference/API
^^^^^^^^^^^^^

See the :ref:`sciserver-ref-scidrive` reference section for all details on the Python wrapper tools.

.. rubric:: Class

.. autosummary:: sciserver.scidrive.SciDrive

.. rubric:: Methods

.. autosummary::

    sciserver.scidrive.SciDrive.createContainer
    sciserver.scidrive.SciDrive.upload
    sciserver.scidrive.SciDrive.download
    sciserver.scidrive.SciDrive.delete
    sciserver.scidrive.SciDrive.directoryList
    sciserver.scidrive.SciDrive.publicUrl
