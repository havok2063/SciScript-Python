# SciScript-Python

## Python libraries for Jupyter Notebooks

This python package provides functions for quick access of [SciServer](http://www.sciserver.org) APIs (web services) and tools.
[SciServer](http://www.sciserver.org) provides a new online framework for data-intensive scientifc computing in the cloud,
where the motto is to bring the computation close where the data is stored, and allow seamless access and sharing of big data sets within the scientific community.

Some SciServer tools you can access with this package:

 * [Login Portal](http://portal.sciserver.org): Single sign-on portal to all SciServer applications.

 * [CasJobs](http://skyserver.sdss.org/CasJobs): Database storage and querying.

 * [SciDrive](http://www.scidrive.org/): Drag-and-drop file storage and sharing.

 * [SkyServer](http://skyserver.sdss.org/): Access to the SDSS astronomical survey.

 * [SkyQuery](http://www.voservices.net/skyquery): Cross-match of astronomical source catalogs.

Maintainer of this repo: Brian Cherinka.

Original Authors: Gerard Lemson, Manuchehr Taghizadeh-Popp.

[![readthedocs](https://readthedocs.org/projects/docs/badge/)](http://sdss-marvin.readthedocs.io/en/latest/)

[Documentation](http://sciserver.readthedocs.io/en/latest/): Python tools for SciServer

Installation
------------

To install:

    pip install sciserver

If you would like to contribute to SciServer's development, you can clone this git repo, pip install the dependencies, and then setup with `python setup.py install`:

    git clone https://github.com/havok2063/SciScript-Python sciserver
    cd sciserver
    pip install -r requirements.txt
    python setup.py install

Examples
--------

In the directory `examples` you can find python scripts or Jupyter notebooks that will run sample code using SciScript-Python modules and methods.

Testing
-------

To run the suite of tests:

    cd sciserver/python/tests
    pytest

