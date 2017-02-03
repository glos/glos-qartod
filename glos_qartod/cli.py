#!/usr/bin/env python
'''
glos_qartod/cli.py
'''
from argparse import ArgumentParser
from netCDF4 import Dataset
from glos_qartod.qc import DatasetQC
from glos_qartod import get_logger
import logging
import logging.config
import pkg_resources
import os
import six
import json
import pandas as pd
from lxml import etree
from redis import StrictRedis
import redis_lock


def main():
    '''
    Apply QARTOD QC to GliderDAC submitted netCDF files
    '''
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument('-c', '--config', help='Path to config YML file to use')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Turn on logging')
    parser.add_argument('netcdf_files', nargs='+',
                        help='NetCDF file to apply QC to')

    args = parser.parse_args()
    if args.verbose:
        setup_logging()
    get_logger().info("Loading config %s", args.config)
    config = pd.read_excel(args.config)
    run_qc_list(config, args.netcdf_files)

def run_qc_list(config, nc_file_list):
    for nc_path in nc_file_list:
        with Dataset(nc_path, 'r') as nc:
            run_qc(config, nc)

def open_and_qc(config, nc_path):
    with Dataset(nc_path, 'r') as nc:
        run_qc(config, nc)


def create_or_open_qc_file(qc_file_name, parent_dimensions):
    """
    Given a name of a QC file, attempt to open it.  If it exists, check
    that it has the same dimensions as "dimensions", primarily taken from
    the raw parent NetCDF dataset and then open it.  If it does not, create a
    new NetCDF file with the specified dimensions.  Returns an opened netCDF
    Dataset object.

    :param qc_file_name str: The path of the file to open or create
    :param dimensions list: A list of tuples with the dimensions
    """
    if os.path.exists(qc_file_name):
        try:
            ncfile = Dataset(qc_file_name, 'a')
            if ncfile.dimensions != parent_dimensions:
                raise ValueError("File {} had dimensional mismatch with original data dimensions, recreating QC file".format(qc_file_name))
            else:
                return ncfile
            # if an exception is returned, log it and fall through to write the
            # QC file from scratch
        except Exception as e:
            ncfile.close()
            get_logger().warning(str(e))

    # if we reached here, either the qc file didn't exist or ran into an error
    # while trying to be opened, so attempt to create the file from scratch
    ncfile = Dataset(qc_file_name, 'w')

    # create dimensions
    for d in six.itervalues(parent_dimensions):
        ncfile.createDimension(d.name, d.size)

    return ncfile




def run_qc(config, ncfile, qc_suffix=None):
    '''
    Runs QC on a netCDF file
    '''
    qc_end = 'ncq' if qc_suffix is None else qc_suffix
    fname_base, ext = os.path.splitext(ncfile.filepath())
    qc_filename = fname_base + '.{}'.format(qc_end)
    # Look for existing qc_file or return new one
    qc_file = create_or_open_qc_file(qc_filename, ncfile.dimensions)
    # load NcML aggregation if it exists
    ncml_filename = fname_base + '.ncml'
    qc = DatasetQC(ncfile, qc_file, ncml_filename, config)
    # zero length times will throw an IndexError in the netCDF interface,
    # and won't result in any QC being applied anyways, so skip them if present
    if len(qc.ncfile.variables['time']) > 0:
        for varname in qc.find_geophysical_variables():
            get_logger().info("Applying QC to %s", varname)
            ncvar = ncfile.variables[varname]
            for qcvarname in qc.create_qc_variables(ncvar):
                qcvar = qc_file.variables[qcvarname]
                get_logger().info(qcvarname)
                qc.apply_qc(qcvar)
            get_logger().info("Primary QC")
            qc.apply_primary_qc(ncvar)
    # if there were changes in the ncml file, write them
    if qc.ncml_write_flag:
        with open(ncml_filename, 'w') as ncml_file:
            ncml_file.write(etree.tostring(qc.ncml))
    qc_file.close()


def run_qc_str(config, nc_path):
    # take out a lock on the file being processed
    with Dataset(nc_path, 'r') as nc:
        run_qc(config, nc)

def run_qc_str_lock(config, nc_path):
    conn = StrictRedis()
    # take out a lock on the file being processed
    with redis_lock.Lock(conn, "{}-lock".format(nc_path)):
        with Dataset(nc_path, 'r') as nc:
            run_qc(config, nc)

def setup_logging(default_path=None, default_level=logging.INFO,
                  env_key='LOG_CFG'):
    """
    Setup logging configuration
    """
    path = default_path or pkg_resources.resource_filename('glos_qartod',
                                                           'logging.json')
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
