.. _sciserver-skyserver:

SkyServer
=========

SkyServer (new window) is the primary public interface to catalog data from the Sloan Digital Sky Survey (SDSS), one of the largest and most successful projects in the history of astronomy. Since 2001, SkyServer has featured interactive navigation through the sky, along with sophisticated query tools to search SDSS imaging and spectroscopic data. SkyServer consists of an extensive web application developed in C# and ASP.NET, using SQL Server as the back end database storage system. It has been developed in a modular fashion from the beginning, using Web Service technologies.

With SciServer, the venerable SkyServer site is about to receive a major upgrade. The new SkyServer uses modern web technologies, and will offer flexible user access through updated web services.

Although there are major changes under the hood, the site continues to function as it always has. You can still search SDSS data in a variety of ways, including writing freeform SQL queries. All SciServer tools will continue to be offered free of charge to everyone.

The new version of SkyServer will includes several new features to enhance your research and teaching, such as the ability to log in and save your queries and data, better integration with CasJobs, and a new drag-and-drop interface for data management that makes uploading your data easier.

Getting Started
^^^^^^^^^^^^^^^

.. _sciserver_skyserver_api:

Reference/API
^^^^^^^^^^^^^

See the :ref:`sciserver-ref-skyserver` reference section for all details on the Python wrapper tools.

.. rubric:: Class

.. autosummary:: sciserver.skyserver.SkyServer

.. rubric:: Methods

.. autosummary::

    sciserver.skyserver.SkyServer.sqlSearch
    sciserver.skyserver.SkyServer.radialSearch
    sciserver.skyserver.SkyServer.rectangularSearch
    sciserver.skyserver.SkyServer.objectSearch
    sciserver.skyserver.SkyServer.getJpegImgCutout

