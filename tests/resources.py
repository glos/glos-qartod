#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
tests/resources.py
'''

from pkg_resources import resource_filename
import os
import subprocess


def get_filename(path):
    '''
    Returns the path to a valid dataset
    '''
    filename = path
    if not os.path.exists(filename):
        cdl_path = filename.replace('.nc', '.cdl')
        generate_dataset(cdl_path, filename)
    return filename


def generate_dataset(cdl_path, nc_path):
    subprocess.call(['ncgen', '-o', nc_path, cdl_path])


STATIC_FILES = {
    'leorgn': get_filename('tests/data/leorgn.nc'),
}
