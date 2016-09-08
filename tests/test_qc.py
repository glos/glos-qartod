#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
tests/test_qc.py
'''

from unittest import TestCase
from glos_qartod.qc import DatasetQC


class TestQC(TestCase):

    def test_loading_config(self):
        config_path = 'tests/data/GLOS-Climatologies.xlsx'

        qc = DatasetQC(None, config_path)
        assert qc.config is not None
        df = qc.config
        variables = df[df['station_id'] == 'leorgn'].variable
        assert u'blue_green_algae' in variables.tolist()
