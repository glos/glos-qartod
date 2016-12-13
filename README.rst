GLOS QARTOD
===========

A small python utility for applying IOOS QARTOD to simple station based netCDF
time-series datasets.

::

   Copyright 2016 Great Lakes Observing System

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

Installing
----------

**Required C libraries**

C libraries for NetCDF and HDF5 are required in order to read and process
NetCDF files.  Either install them via your preferred package manager or
from source.

An accessible Redis server is required if using the batch processing mode.
(see below)

To install Python dependencies for the project, run
`pip install -r requirements.txt`.
It is recommended to have the latest version of pip installed to take advantage
of the binary packages for libraries such as pandas and numpy.

Testing
-------

Usage
-----

An excel file is required to configure the QARTOD tests.

There are two sheets, "Variable Config" and "Mappings".
"Variable Config" has the following structure to define tests:

`station_id,variable,units,test1.param1,test1.param2,...testp.paramq`

"Mappings" has only two columns: var_name and var_dir.  The table is used for
when variable names do not match the folder names for the variable.
For example, some stations may have a "turbidity" variable, but are stored
under a folder named "ysi_turbidity".  This is currently used for the `run.py`
script, which attempts to recurse into subdirectories based on the variable and
station name specified in "Variable Config".

There are two main ways of invoking the QC checks:

`python run.py <excel_config.xlsx> <root_folder>`

In this configuration, each row in the "Variable Config" is parsed.
For each station and variable, any NetCDF files are found and the defined
QARTOD tests in the config are applied.  This configuration is primarily used
for archived data.  It checks for the presence of all the defined QC variables,
and runs any not present.  If all the QC checks are present, the file is not
added to the list of files to be processed.  The individual files to be
processed are then pushed to the Redis job queue.  Note that prior to pushing
to the jobs queue, the checks only determine whether the check variables are
present, it does not ensure that QC has been recently applied.  Thus it is more
appropriate to use this command to QC archived data against a set
configuration.

`python cli.py -c <excel_config.xlsx> <netcdf_file1.nc> ... <netcdf_filen.nc>`

Runs QC against a single NetCDF file with the QC read from the configuration.
Matches against the station and variable and runs any QC necessary.  As this
runs QC against an entire file, this is most appropriate for indivivdual files
or files updated in near real-time.
