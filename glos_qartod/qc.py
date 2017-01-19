#!/usr/bin/env python
'''
glider_qc/glider_qc.py
'''
import numpy as np
import numpy.ma as ma
import quantities as pq
import pandas as pd
import json
from cf_units import Unit
from netCDF4 import num2date
from ioos_qartod.qc_tests import qc
from ioos_qartod.qc_tests import gliders as gliders_qc
from glos_qartod import get_logger


class DatasetQC(object):
    def __init__(self, ncfile, config_file):
        self.ncfile = ncfile
        self.load_config(config_file)

    def find_geophysical_variables(self):
        '''
        Returns a list of variables that match any variables listed in the
        config file for this station
        '''
        variables = []
        station_name = self.find_station_name()
        station_id = station_name.split(':')[-1]
        get_logger().info("Station ID: %s", station_id)
        #local_config = self.config[self.config['station_id'].astype(str).isin({station_id, '*'})]
        local_config = self.config[self.config['station_id'].astype(str) == station_id]
        # get remaining "all" config
        univ_config = self.config[(self.config['station_id'] == '*') & (~self.config['variable'].isin(local_config['variable']))]
        # switch over to normal station ID
        univ_config['station_id'] = station_id
        config_all = pd.concat([local_config, univ_config])

        configured_variables = config_all.variable.tolist()
        get_logger().info("Configured variables: %s", ', '.join(configured_variables))
        return set(configured_variables).intersection(self.ncfile.variables)

    def find_station_name(self):
        '''
        Returns the station identifier using the ioos_code attribute
        '''
        platform_name = self.ncfile.platform
        if platform_name not in self.ncfile.variables:
            raise ValueError("Platform defined as %s but no variable matches this name" % platform_name)
        station_id = self.ncfile.variables[platform_name].ioos_code
        return station_id

    def find_ancillary_variables(self, ncvariable):
        '''
        Returns the valid ancillary variables associated with a particular
        variable.

        :param netCDF4.Variable ncvariable: Variable to get the ancillary
                                            variables for
        '''
        valid_variables = []
        ancillary_variables = getattr(ncvariable, 'ancillary_variables', '').split(' ')
        for varname in ancillary_variables:
            # Skip the standard GliderDAC
            if varname == '%s_qc' % ncvariable.name:
                continue
            if varname in self.ncfile.variables:
                valid_variables.append(varname)
        return valid_variables

    def append_ancillary_variable(self, parent, child):
        '''
        Links two variables through the ancillary_variables attribute

        :param netCDF.Variable parent: Parent Variable
        :param netCDF.Variable child: Status Flag Variable
        '''

        ancillary_variables = getattr(parent, 'ancillary_variables', '').split(' ')
        # only add the ancillary variable name if it is not already present
        if child.name not in ancillary_variables:
            ancillary_variables.append(child.name)
            parent.ancillary_variables = ' '.join(ancillary_variables)

    def needs_qc(self, ncvariable):
        '''
        Returns True if the variable has no associated QC variables

        :param netCDF4.Variable ncvariable: Variable to get the ancillary
                                            variables for
        '''
        ancillary_variables = self.find_ancillary_variables(ncvariable)
        return len(ancillary_variables) == 0

    def create_qc_variables(self, ncvariable):
        '''
        Returns a list of variable names for the newly created variables for QC flags
        '''
        name = ncvariable.name
        standard_name = ncvariable.standard_name
        dims = ncvariable.dimensions

        # STYLE: consider DRYing up
        templates = {
            'flat_line': {
                'name': 'qartod_%(name)s_flat_line_flag',
                'long_name': 'QARTOD Flat Line Test for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf',
                'qartod_test': 'flat_line'
            },
            'gross_range': {
                'name': 'qartod_%(name)s_gross_range_flag',
                'long_name': 'QARTOD Gross Range Test for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf',
                'qartod_test': 'gross_range'
            },
            'rate_of_change': {
                'name': 'qartod_%(name)s_rate_of_change_flag',
                'long_name': 'QARTOD Rate of Change Test for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf',
                'qartod_test': 'rate_of_change'
            },
            'spike': {
                'name': 'qartod_%(name)s_spike_flag',
                'long_name': 'QARTOD Spike Test for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf',
                'qartod_test': 'spike'
            },
            'pressure': {
                'name': 'qartod_monotonic_pressure_flag',
                'long_name': 'QARTOD Pressure Test for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf',
                'qartod_test': 'pressure'
            },
            'primary': {
                'name': 'qartod_%(name)s_primary_flag',
                'long_name': 'QARTOD Primary Flag for %(standard_name)s',
                'standard_name': '%(standard_name)s status_flag',
                'flag_values': np.array([1, 2, 3, 4, 9], dtype=np.int8),
                'flag_meanings': 'GOOD NOT_EVALUATED SUSPECT BAD MISSING',
                'references': 'http://gliders.ioos.us/static/pdf/Manual-for-QC-of-Glider-Data_05_09_16.pdf'
            }
        }

        qcvariables = []

        for tname, template in templates.items():
            if tname == 'pressure' and standard_name != 'sea_water_pressure':
                continue
            variable_name = template['name'] % {'name': name}

            if variable_name not in self.ncfile.variables:
                ncvar = self.ncfile.createVariable(variable_name, np.int8, dims, fill_value=np.int8(9))
            else:
                ncvar = self.ncfile.variables[variable_name]

            ncvar.units = '1'
            ncvar.standard_name = template['standard_name'] % {'standard_name': standard_name}
            ncvar.long_name = template['long_name'] % {'standard_name': standard_name}
            ncvar.flag_values = template['flag_values']
            ncvar.flag_meanings = template['flag_meanings']
            ncvar.references = template['references']
            if 'qartod_test' in template:
                ncvar.qartod_test = template['qartod_test']
            qcvariables.append(variable_name)
            self.append_ancillary_variable(ncvariable, ncvar)

        return qcvariables

    def load_config(self, path):
        '''
        Returns a dataframe loaded from the excel config file.
        '''
        get_logger().info("Loading config %s", path)
        df = pd.read_excel(path)
        self.config = df
        return df

    def apply_qc(self, ncvariable):
        '''
        Applies QC to a qartod variable

        :param netCDF4.Variable ncvariable: A QARTOD Variable
        '''
        qc_tests = {
            'flat_line': qc.flat_line_check,
            'gross_range': qc.range_check,
            'rate_of_change': qc.rate_of_change_check,
            'spike': qc.spike_check,
            'pressure': gliders_qc.pressure_check
        }

        # If the qartod_test attribute isn't defined then this isn't a variable
        # this script created and is not eligble for automatic QC
        qartod_test = getattr(ncvariable, 'qartod_test', None)
        if not qartod_test:
            return

        # Get a reference to the parent variable using the standard_name
        # attribute
        standard_name = getattr(ncvariable, 'standard_name').split(' ')[0]
        parent = self.ncfile.get_variables_by_attributes(standard_name=\
                                                         standard_name)[0]

        test_params = self.get_test_params(parent.name)
        # If there are no parameters defined for this test, don't apply QC
        if qartod_test not in test_params:
            return

        test_params = test_params[qartod_test]

        if 'thresh_val' in test_params:
            test_params['thresh_val'] = test_params['thresh_val'] / pq.hour

        times, values, mask = self.get_unmasked(parent)

        if qartod_test in ('rate_of_change', 'spike'):
            times = ma.getdata(times[~mask])
            if times.size > 0:
               dates = np.array(num2date(times,
                                         self.ncfile.variables['time'].units),
                                dtype='datetime64[ms]')
            else:
               dates = np.array([], dtype='datetime64[ms]')
            test_params['times'] = dates

        if qartod_test == 'pressure':
            test_params['pressure'] = values
        else:
            test_params['arr'] = values

        if values.size > 0:
           qc_flags = qc_tests[qartod_test](**test_params)
        else:
           qc_flags = np.array([], dtype=np.uint8)

        get_logger().info("Flagged: %s", len(np.where(qc_flags == 4)[0]))
        get_logger().info("Total Values: %s", len(values))
        ncvariable[~mask] = qc_flags

    def get_unmasked(self, ncvariable):
        times = self.ncfile.variables['time'][:]
        values = ncvariable[:]

        mask = np.zeros(times.shape[0], dtype=bool)

        if hasattr(values, 'mask'):
            mask |= values.mask

        if hasattr(times, 'mask'):
            mask |= times.mask

        values_initial = ma.getdata(values[~mask])
        config = self.get_config(ncvariable.name)
        units = getattr(ncvariable, 'units', '1')
        # If units are not defined or empty, set them to unitless
        # If the config units are empty, do not attempt to convert units
        # The latter is necessary as some of the NetCDF files do not have
        # units attribute under the udunits variable definitions
        if not units or pd.isnull(config.units):
            units = '1'
            values = values_initial
        # must be a CF unit or this will throw an exception
        elif ncvariable.units != config.units:
            try:
                values = Unit(units).convert(values_initial, config.units)
            except ValueError as e:
                exc_text = "Caught exception while converting units: {}".format(e)
                get_logger().warn(exc_text)
                values = values_initial
        return times, values, mask

    def get_gross_range_config(self, config):
        '''
        Returns a dictionary of test configuration parameters for the given config row

        :param config: A row from the pandas dataframe representing the configuration
        '''
        gross_range = {}
        if ('gross_range.sensor_min' in config and not pd.isnull(config['gross_range.sensor_min'])) and \
           ('gross_range.sensor_max' in config and not pd.isnull(config['gross_range.sensor_max'])):
            gross_range['sensor_span'] = [
                config['gross_range.sensor_min'],
                config['gross_range.sensor_max']
            ]
        if ('gross_range.user_min' in config and not pd.isnull(config['gross_range.user_min'])) and \
           ('gross_range.user_max' in config and not pd.isnull(config['gross_range.user_max'])):
            gross_range['user_span'] = [
                config['gross_range.user_min'],
                config['gross_range.user_max']
            ]
        return gross_range

    def get_rate_of_change_config(self, config):
        '''
        Returns a dictionary of test configuration parameters for the given config row

        :param config: A row from the pandas dataframe representing the configuration
        '''
        rate_of_change = {}
        if ('rate_of_change.threshold' in config and not pd.isnull(config['rate_of_change.threshold'])):
            rate_of_change['thresh_val'] = config['rate_of_change.threshold']
        return rate_of_change

    def get_spike_config(self, config):
        '''
        Returns a dictionary of test configuration parameters for the given config row

        :param config: A row from the pandas dataframe representing the configuration
        '''
        spike = {}

        if ('spike.low_threshold' in config and not pd.isnull(config['spike.low_threshold'])):
            spike['low_thresh'] = config['spike.low_threshold']
        if ('spike.high_threshold' in config and not pd.isnull(config['spike.high_threshold'])):
            spike['high_thresh'] = config['spike.high_threshold']

        return spike

    def get_flat_line_config(self, config):
        '''
        Returns a dictionary of test configuration parameters for the given config row

        :param config: A row from the pandas dataframe representing the configuration
        '''
        flat_line = {}

        if ('flat_line.low_reps' in config and not pd.isnull(config['flat_line.low_reps'])):
            flat_line['low_reps'] = int(config['flat_line.low_reps'])
        if ('flat_line.high_reps' in config and not pd.isnull(config['flat_line.high_reps'])):
            flat_line['high_reps'] = int(config['flat_line.high_reps'])
        if ('flat_line.epsilon' in config and not pd.isnull(config['flat_line.epsilon'])):
            flat_line['eps'] = config['flat_line.epsilon']

        # Default epsilon value
        if flat_line and 'eps' not in flat_line:
            flat_line['eps'] = 1.1920929e-07

        return flat_line

    def get_test_params(self, variable):
        '''
        Returns a dictionary of test parameters based on configuration

        :param netCDF4.Variable ncvariable: NCVariable
        '''
        config = self.get_config(variable)
        test_params = {}

        gross_range = self.get_gross_range_config(config)
        if gross_range:
            test_params['gross_range'] = gross_range

        rate_of_change = self.get_rate_of_change_config(config)
        if rate_of_change:
            test_params['rate_of_change'] = rate_of_change

        spike = self.get_spike_config(config)
        if spike:
            test_params['spike'] = spike

        flat_line = self.get_flat_line_config(config)
        if flat_line:
            test_params['flat_line'] = flat_line

        return test_params

    def get_config(self, variable):
        '''
        Returns a row of the config data frame for the station and variable

        :param netCDF4.Variable ncvariable: NCVariable
        '''
        station_name = self.find_station_name()
        station_id = station_name.split(':')[-1]
        rows = self.config[(self.config['station_id'].astype(str) == station_id) &
                           (self.config['variable'] == variable) |
                           (self.config['station_id'].astype(str) == '*') &
                           (self.config['variable'] == variable)]
        dedup = rows.sort(['variable', 'station_id'], ascending=[True,
                                         False]).drop_duplicates('variable')
        dedup['station_id'] = station_id
        if len(dedup) > 0:
            return dedup.iloc[-1]
        raise KeyError("No configuration found for %s and %s" % (station_id,
                                                                 variable))

    def apply_primary_qc(self, ncvariable):
        '''
        Applies the primary QC array which is an aggregate of all the other QC
        tests.

        :param netCDF4.Variable ncvariable: NCVariable
        '''
        primary_qc_name = 'qartod_%s_primary_flag' % ncvariable.name
        if primary_qc_name not in self.ncfile.variables:
            return

        qcvar = self.ncfile.variables[primary_qc_name]

        ancillary_variables = self.find_ancillary_variables(ncvariable)
        vectors = []

        for qc_variable in ancillary_variables:
            if qc_variable == primary_qc_name:
                continue
            ncvar = self.ncfile.variables[qc_variable]
            vectors.append(ma.getdata(ncvar[:]))

        flags = qc.qc_compare(vectors)
        qcvar[:] = flags
