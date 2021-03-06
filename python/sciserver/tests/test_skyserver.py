# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-08-06 19:53:26
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-08-30 10:35:59

from __future__ import print_function, division, absolute_import
import pytest
import skimage
import os

# sky test data
SkyServer_TestQuery = "select top 1 specobjid, ra, dec from specobj order by specobjid"
SkyServer_DataRelease = "DR13"
SkyServer_QueryResultCSV = "specobjid,ra,dec\n299489677444933632,146.71421,-1.0413043\n"
SkyServer_RadialSearchResultCSV = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n1237671939804561654,6162,301,3,133,246,3,258.250804,64.051445,23.339820,22.319400,21.411050,21.119710,20.842770,0.664019,0.116986,0.076410,0.080523,0.238198\n'
SkyServer_RectangularSearchResultCSV = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n1237671939804628290,6162,301,3,134,1346,6,258.304721,64.006203,25.000800,24.500570,22.485400,21.103450,20.149990,0.995208,0.565456,0.166184,0.071836,0.124986\n'
SkyServer_ObjectSearchResultObjID = 1237671939804561654


class TestSkyServer(object):

    def test_sqlsearch(self, sky):
        df = sky.sqlSearch(sql=SkyServer_TestQuery, dataRelease=SkyServer_DataRelease)
        assert SkyServer_QueryResultCSV == df.to_csv(index=False)

    def test_getjpegimgcutout(self, sky):
        img = sky.getJpegImgCutout(ra=197.614455642896, dec=18.438168853724, width=512,
                                   height=512, scale=0.4, dataRelease=SkyServer_DataRelease,
                                   opt="OG", query="SELECT TOP 100 p.objID, p.ra, p.dec, p.r FROM fGetObjFromRectEq(197.6,18.4,197.7,18.5) n, PhotoPrimary p WHERE n.objID=p.objID")
        testimg = os.path.join(os.path.dirname(__file__), 'data/TestGalaxy.jpeg')
        im = skimage.io.imread(testimg)
        assert img.tobytes() == im.tobytes()

    def test_radialsearch(self, sky):
        df = sky.radialSearch(ra=258.25, dec=64.05, radius=0.1, dataRelease=SkyServer_DataRelease)
        assert SkyServer_RadialSearchResultCSV == df.to_csv(index=False, float_format="%.6f")

    def test_rectangularsearch(self, sky):
        df = sky.rectangularSearch(min_ra=258.3, max_ra=258.31, min_dec=64, max_dec=64.01, dataRelease=SkyServer_DataRelease)
        assert SkyServer_RectangularSearchResultCSV == df.to_csv(index=False, float_format="%.6f")

    def test_objectsearch(self, sky):
        object = sky.objectSearch(ra=258.25, dec=64.05, dataRelease=SkyServer_DataRelease)
        assert SkyServer_ObjectSearchResultObjID == object[0]["Rows"][0]["id"]

