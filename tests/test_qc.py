#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
tests/test_qc.py
'''

from unittest import TestCase
from glos_qartod.qc import DatasetQC
from netCDF4 import Dataset
from tests.resources import STATIC_FILES


class TestQC(TestCase):

    def setUp(self):
        self.config_path = 'tests/data/GLOS-Climatologies.xlsx'

    def test_loading_config(self):

        qc = DatasetQC(None, self.config_path)
        assert qc.config is not None
        df = qc.config
        variables = df[df['station_id'] == 'leorgn'].variable
        assert u'blue_green_algae' in variables.tolist()

    def test_find_station_name(self):
        with Dataset(STATIC_FILES['leorgn'], 'r') as nc:
            qc = DatasetQC(nc, self.config_path)
            station_name = qc.find_station_name()
        assert station_name == "urn:ioos:station:glos:leorgn"

    def test_find_geophysical_variables(self):
        with Dataset(STATIC_FILES['leorgn'], 'r') as nc:
            qc = DatasetQC(nc, self.config_path)
            variables = qc.find_geophysical_variables()
        assert u'blue_green_algae' in variables

