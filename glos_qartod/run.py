import os
import sys
import glob
import pandas as pd
from redis import Redis
from rq import Queue
from netCDF4 import Dataset
from glos_qartod import cli
from glos_qartod import get_logger


def main():
    q = Queue(connection=Redis())
    conf_file, proc_dir = sys.argv[1:3]
    sheets = pd.read_excel(conf_file, None)
    conf = sheets['Variable Config']
    mappings = sheets['Mappings'].set_index('var_name').to_dict()['var_dir']
    files = qc_subset(proc_dir, conf, mappings)
    for f in files:
        q.enqueue(cli.run_qc_str_lock, conf_file, f)

def qc_subset(dir_root, conf, mappings):
    """Returns a subset of the files to QC based on whether there are
       defined keys, etc"""
    files = []
    for row in conf.iterrows():
        vals = row[1]
        # get all QC keys, i.e. not station, variable, units
        qc_keys = vals.iloc[3:]
        # get unique tests that are defined for this station/variable
        qc_nn = {k.split('.')[0] for k in qc_keys[qc_keys.notnull()].keys()}
        # if empty set, i.e. all keys are null, skip processing the batch of
        # files
        if not qc_nn:
           continue

        # check for existing variable names in the file.  If they all already
        # exist, then
        var = vals['variable']
        station = str(vals['station_id'])
        var_dir = mappings.get(var, var)
        # form a set of the variable names
        qc_varnames = {"qartod_{}_{}_flag".format(var, n) for n in qc_nn}
        # some of the variables are named inconsistently, so fall back
        # to the directory names if need be
        qc_varnames_bkp = {"qartod_{}_{}_flag".format(var_dir, n) for n in
                           qc_nn}
        # get any remapped directories for this variable name, or if none exist,
        # get the varaiable name as the target directory
        # get the directory matching this station name or get all if * glob
        # is used
        station_dirs = glob.glob(os.path.join(dir_root, var_dir, station))
        for dest_dir in station_dirs:
            files.extend(find_files(dest_dir, qc_varnames, qc_varnames_bkp))

    return set(files)


def find_files(dest_dir, qc_varnames, qc_varnames_bkp):
    nc_files = []
    if os.path.exists(dest_dir):
        for root, subdir, fname in os.walk(dest_dir):
            # find all .nc files in this directory, check if there has been
            # qc applied to them, and create absolute paths to them
            # path to them
            if not fname.endswith('.nc'):
                continue

            full_path = os.path.join(root, fname)
            if not check_if_qc_vars_exist(full_path, qc_varnames,
                                          qc_varnames_bkp):
                nc_files.append(full_path)
    else:
        get_logger().warn("Directory '{}' does not exist but was referenced in config".format(dest_dir))

    return nc_files

def check_if_qc_vars_exist(file_path, qc_varnames, qc_varnames_bkp):
    """
    Checks that QC variables exist in the corresponding QC file based on
    data file's filename.
    Returns False if not all the QC variables are present, and True
    if they are.
    """

    qc_filepath = file_path.rsplit('.', 1)[0] + '.ncq'
    # try to fetch the QC file's variable names.  If it does not
    # exist, no QC has been applied and it must be created later
    if not os.path.exists(qc_filepath):
        return False
    else:
        try:
            with Dataset(qc_filepath) as f:
                qc_vars = f.variables.keys()
            # check if all the QC variables exist in the file.
            # if they don't, add them to the list of files to be processed
            return (qc_varnames.issubset(qc_vars) or
                    qc_varnames_bkp.issubset(qc_vars))
        # if for some reason we can't open the file,
        # note the exception and treat the qc variables as empty
        except:
            get_logger().exception('Failed to open file {}'.format(qc_filepath))
            return False

if __name__ == '__main__':
    main()
