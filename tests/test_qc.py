#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
tests/test_qc.py
'''
from __future__ import print_function
from __future__ import unicode_literals

from unittest import TestCase
from glos_qartod.qc import DatasetQC
from netCDF4 import Dataset
from tests.resources import STATIC_FILES, get_filename

import numpy as np
import os


class TestQC(TestCase):

    def setUp(self):
        self.config_path = 'tests/data/GLOS-Climatologies.xlsx'

    def reload_file(self):
        os.remove(STATIC_FILES['leorgn'])
        get_filename('tests/data/leorgn.nc')
        assert os.path.exists(STATIC_FILES['leorgn'])

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

    def test_get_config(self):
        with Dataset(STATIC_FILES['leorgn'], 'r') as nc:
            qc = DatasetQC(nc, self.config_path)
            config = qc.get_config('blue_green_algae')
            assert config.units == u'rfu'
            assert config['gross_range.sensor_min'] == -1

    def test_get_unmasked(self):
        with Dataset(STATIC_FILES['leorgn'], 'r') as nc:
            qc = DatasetQC(nc, self.config_path)
            variable = nc.variables['blue_green_algae']

            times, values, mask = qc.get_unmasked(variable)
            np.testing.assert_allclose(values[:4], np.array([0.13, 0.12, 0.13, 0.08], dtype='f'))

    def test_apply_qc(self):
        self.addCleanup(self.reload_file)
        with Dataset(STATIC_FILES['leorgn'], 'r+') as nc:
            qc = DatasetQC(nc, self.config_path)
            variable = nc.variables['blue_green_algae']

            for qcvarname in qc.create_qc_variables(variable):
                qcvar = nc.variables[qcvarname]
                qc.apply_qc(qcvar)

            qc.apply_primary_qc(variable)

