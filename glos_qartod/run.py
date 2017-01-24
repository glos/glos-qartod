import os
import sys
import glob
import pandas as pd
from redis import Redis
from rq import Queue
from netCDF4 import Dataset
from glos_qartod import cli


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
    files = []
    if os.path.exists(dest_dir):
        for root, subdir, fname in os.walk(dest_dir):
            for nc_file in (f for f in fname if f.endswith('.nc')):
                # TODO: Add some sort of caching check.
                file_dest = os.path.join(root, nc_file)
                # check if all the variables already exist in the file
                ds = Dataset(file_dest)
                # if all of the variables for the expected QC variables
                # aren't present in the dataset, add the file to the list
                # of files to be QCed
                # sometimes naming conventions are inconsistent, so # check both the remapped version and the regular version
                if not (qc_varnames.issubset(ds.variables) or
                        qc_varnames_bkp.issubset(ds.variables)):
                    files.append(file_dest)
    else:
        # TODO: Add logging noting nonexistent directory
        pass

    return files


if __name__ == '__main__':
    main()
