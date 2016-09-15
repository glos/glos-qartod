#!/usr/bin/env python
'''
glos_qartod/cli.py
'''
from __future__ import print_function
from argparse import ArgumentParser
from netCDF4 import Dataset
from glos_qartod.qc import DatasetQC
from glos_qartod import get_logger
import sys
import logging
import logging.config
import pkg_resources
import os
import json


def main():
    '''
    Apply QARTOD QC to GliderDAC submitted netCDF files
    '''
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('-c', '--config', help='Path to config YML file to use')
    parser.add_argument('netcdf_files', nargs='+', help='NetCDF file to apply QC to')
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on logging')

    args = parser.parse_args()
    if args.verbose:
        setup_logging()
    for nc_path in args.netcdf_files:
        print(nc_path)
        with Dataset(nc_path, 'r+') as nc:
            run_qc(args.config, nc)
    sys.exit(0)


def run_qc(config, ncfile):
    '''
    Runs QC on a netCDF file
    '''
    qc = DatasetQC(ncfile, config)
    for varname in qc.find_geophysical_variables():
        get_logger().info("Applying QC to %s", varname)
        ncvar = ncfile.variables[varname]
        for qcvarname in qc.create_qc_variables(ncvar):
            qcvar = ncfile.variables[qcvarname]
            get_logger().info(qcvarname)
            qc.apply_qc(qcvar)
        get_logger().info("Primary QC")
        qc.apply_primary_qc(ncvar)


def setup_logging(
    default_path=None,
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path or pkg_resources.resource_filename('glos_qartod', 'logging.json')
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


if __name__ == '__main__':
    main()

