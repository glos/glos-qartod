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
from collections import OrderedDict


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
    for nc_file in args.netcdf_files:
        with Dataset(nc_file, 'r') as nc:
            run_qc(config, nc)


def extract_dimensions(od):
    """Extract comparable information from netCDF4 dimension OrderedDict"""
    return OrderedDict((v.name, v.size) for v in six.itervalues(od))

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
            if (extract_dimensions(ncfile.dimensions) !=
                extract_dimensions(parent_dimensions)):
                raise ValueError("File {} had dimensional mismatch with original data dimensions, recreating QC file".format(qc_file_name))
            else:
                return ncfile
            # if an exception is returned, log it and fall through to write the
            # QC file from scratch
        except:
            get_logger().exception("Error during handling of QC file")

    # if we reached here, either the qc file didn't exist or ran into an error
    # while trying to be opened, so attempt to create the file from scratch
    ncfile = Dataset(qc_file_name, 'w')

    # create dimensions
    for d in six.itervalues(parent_dimensions):
        ncfile.createDimension(d.name, d.size)

    return ncfile




def run_qc(config, ncfile, qc_extension='ncq'):
    '''
    Runs QC on a netCDF file

    Takes a path to an Excel file to be read or pandas DataFrame containing
    QC configuration and applies the QC to the file at `filepath`.
    Creates a file with the same base name as filepath, but with the file
    extension specified by `qc_extension`.

    :param config: str or pandas.DataFrame
    :param ncfile: netCDF4.Dataset
    :param qc_extension: str
    '''
    fname_base = ncfile.filepath().rsplit('.', 1)[0]
    qc_filename = "{}.{}".format(fname_base, qc_extension)
    # Look for existing qc_file or return new one
    qc_file = create_or_open_qc_file(qc_filename, ncfile.dimensions)
    # load NcML aggregation if it exists
    ncml_filename = fname_base + '.ncml'
    qc = DatasetQC(ncfile, qc_file, ncml_filename, config)
    # zero length times will throw an IndexError in the netCDF interface,
    # and won't result in any QC being applied anyways, so skip them if present
    if qc.ncfile.variables['time'].size > 0:
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


def run_qc_str(config, nc_path, qc_extension='ncq'):
    """
    Helper function to run_qc.  Mainly used to pass jobs off from redis.

    :param config: str or pandas.DataFrame
    :param nc_path: str
    :param qc_extension: str
    """
    # take out a lock on the file being processed
    with Dataset(nc_path, 'r') as nc:
        run_qc(config, nc, qc_extension)

def run_qc_str_lock(config, nc_path):
    """
    Helper function to run_qc.  Mainly used to pass jobs off from redis.
    Also takes out a lock in redis to avoid possibly starting multiple jobs
    and causing race conditions.

    :param config: str or pandas.DataFrame
    :param nc_path: str
    :param qc_extension: str
    """
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
