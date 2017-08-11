# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-04 14:25:03
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-10 23:08:13

from __future__ import print_function, division, absolute_import
from setuptools import setup, find_packages
import os

requirements_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
install_requires = [line.strip().replace('==', '>=') for line in open(requirements_file)
                    if not line.strip().startswith('#') and line.strip() != '']

NAME = 'sciserver'
VERSION = '1.11.0dev'

setup(
    name=NAME,
    version=VERSION,
    license='BSD3',
    description='Python toolsuite for the SciServer product',
    author='SciServer Team',
    keywords='sdss sciserver',
    url='https://github.com/havok2063/SciScript-Python',
    packages=find_packages(where='python', exclude=['*egg-info']),
    package_dir={'': 'python'},
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: MacOS X',
        'Framework :: Jupyter',
        'Intended Audience :: Education',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Database :: Front-Ends',
        'Topic :: Documentation :: Sphinx',
        'Topic :: Education :: Computer Aided Instruction (CAI)',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Scientific/Engineering :: Astronomy',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: User Interfaces'
    ],
)
