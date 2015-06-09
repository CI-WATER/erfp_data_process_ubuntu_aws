#!/usr/bin/env python
"""Copies data from RAPID netCDF output to a CF-compliant netCDF file.

Remarks:
    A new netCDF file is created with data from RAPID [1] simulation model
    output. The result follows CF conventions [2] with additional metadata
    prescribed by the NODC timeSeries Orthogonal template [3] for time series
    at discrete point feature locations.

    This script was created for the National Flood Interoperability Experiment,
    and so metadata in the result reflects that.

Requires:
    netcdf4-python - https://github.com/Unidata/netcdf4-python

Inputs:
    Lookup CSV table with COMID, Lat, Lon, and Elev_m columns. Columns must
    be in that order and these must be the first four columns. The order of
    COMIDs in the table must match the order of features in the netCDF file.

    RAPID output netCDF file. File must be named *YYYYMMDDTHHMMZ.nc, e.g.,
    rapid_20150124T0000Z.nc. The ISO datetime indicates the first time
    coordinate in the file. An example CDL text representation of the file
    header is shown below. The file must be located in the 'input' folder.

    Input files are moved to the 'archive' upon completion.

///////////////////////////////////////////////////
netcdf result_2014100520141101 {
dimensions:
    Time = UNLIMITED ; // (224 currently)
    COMID = 61818 ;
variables:
    float Qout(Time, COMID) ;
///////////////////////////////////////////////////

Outputs:
    CF-compliant netCDF file of RAPID results, named with original filename
    with "_CF" appended to the filename. File is written to 'output' folder.

    Input netCDF file is archived or deleted, based on 'archive' config
    parameter.

Usage:
    Option 1: Run standalone. Script will use logger.
    Option 2: Run from another script.
        First, import the script, e.g., import make_CF_RAPID_output as cf.
        If you want to use this script's logger (optional):
            1. Call init_logger with the full path of a log filename to get a
               logger designed for use with this script.
            2. Call main() with the logger as the first argument.
        If you don't want to use the logger, just call main().

References:
    [1] http://rapid-hub.org/
    [2] http://cfconventions.org/
    [3] http://www.nodc.noaa.gov/data/formats/netcdf/v1.1/
"""

import ConfigParser
from datetime import datetime, timedelta
from glob import glob
import os
import re
import shutil
from netCDF4 import Dataset
import numpy as np


def get_this_path():
    """Returns path to the main script being executed.

    Remarks:
        When run as scheduled task, os.getcwd() returns C:\Windows\system32.
        This means if you leave out the full path to files, path defaults to
        system32. If you want to access files relative to the current script,
        you can use the path to __file__, but sometimes (don't know why) this
        returns NameError: name '__file__' is not defined. This function has
        a workaround to get you the path to the current script no matter how
        you are running the script.
    """

    try:
        return os.path.dirname(os.path.realpath(__file__))
    except:
        return os.getcwd()


def log(message, severity):
    """Logs, prints, or raises a message.

    Arguments:
        message -- message to report
        severity -- string of one of these values:
            CRITICAL|ERROR|WARNING|INFO|DEBUG
    """

    print_me = ['WARNING', 'INFO', 'DEBUG']
    if severity in print_me:
        print message
    else:
        raise Exception(message)


def initialize_output(filename, id_dim_name, flow_var_name, time_len,
                      id_len, time_step_seconds):
    """Creates netCDF file with CF dimensions and variables, but no data.

    Arguments:
        filename -- full path and filename for output netCDF file
        id_dim_name -- name of Id dimension and variable, e.g., COMID
        flow_var_name -- name of streamflow variable, e.g., Qout
        time_len -- (integer) length of time dimension (number of time steps)
        id_len -- (integer) length of Id dimension (number of time series)
        time_step_seconds -- (integer) number of seconds per time step
    """

    # Create dimensions
    cf_nc = Dataset(filename, 'w', format='NETCDF3_CLASSIC')
    cf_nc.createDimension('time', time_len)
    cf_nc.createDimension(id_dim_name, id_len)

    # Create variables
    timeSeries_var = cf_nc.createVariable(id_dim_name, 'i4', (id_dim_name,))
    timeSeries_var.long_name = (
        'Unique NHDPlus COMID identifier for each river reach feature')
    timeSeries_var.cf_role = 'timeseries_id'

    time_var = cf_nc.createVariable('time', 'i4', ('time',))
    time_var.long_name = 'time'
    time_var.standard_name = 'time'
    time_var.units = 'seconds since 1970-01-01 00:00:00 0:00'
    time_var.axis = 'T'

    lat_var = cf_nc.createVariable('lat', 'f8', (id_dim_name,),
                                   fill_value=-9999.0)
    lat_var.long_name = 'latitude'
    lat_var.standard_name = 'latitude'
    lat_var.units = 'degrees_north'
    lat_var.axis = 'Y'

    lon_var = cf_nc.createVariable('lon', 'f8', (id_dim_name,),
                                   fill_value=-9999.0)
    lon_var.long_name = 'longitude'
    lon_var.standard_name = 'longitude'
    lon_var.units = 'degrees_east'
    lon_var.axis = 'X'

    z_var = cf_nc.createVariable('z', 'f8', (id_dim_name,),
                                 fill_value=-9999.0)
    z_var.long_name = ('Elevation referenced to the North American ' +
                       'Vertical Datum of 1988 (NAVD88)')
    z_var.standard_name = 'surface_altitude'
    z_var.units = 'm'
    z_var.axis = 'Z'
    z_var.positive = 'up'

    q_var = cf_nc.createVariable(flow_var_name, 'f4', (id_dim_name, 'time'))
    q_var.long_name = 'Discharge'
    q_var.units = 'm^3/s'
    q_var.coordinates = 'time lat lon z'
    q_var.grid_mapping = 'crs'
    q_var.source = ('Generated by the Routing Application for Parallel ' +
                    'computatIon of Discharge (RAPID) river routing model.')
    q_var.references = 'http://rapid-hub.org/'
    q_var.comment = ('lat, lon, and z values taken at midpoint of river ' +
                     'reach feature')

    crs_var = cf_nc.createVariable('crs', 'i4')
    crs_var.grid_mapping_name = 'latitude_longitude'
    crs_var.epsg_code = 'EPSG:4269'  # NAD83, which is what NHD uses.
    crs_var.semi_major_axis = 6378137.0
    crs_var.inverse_flattening = 298.257222101

    # Create global attributes
    cf_nc.featureType = 'timeSeries'
    cf_nc.Metadata_Conventions = 'Unidata Dataset Discovery v1.0'
    cf_nc.Conventions = 'CF-1.6'
    cf_nc.cdm_data_type = 'Station'
    cf_nc.nodc_template_version = (
        'NODC_NetCDF_TimeSeries_Orthogonal_Template_v1.1')
    cf_nc.standard_name_vocabulary = ('NetCDF Climate and Forecast (CF) ' +
                                      'Metadata Convention Standard Name ' +
                                      'Table v28')
    cf_nc.title = 'RAPID Result'
    cf_nc.summary = ("Results of RAPID river routing simulation. Each river " +
                     "reach (i.e., feature) is represented by a point " +
                     "feature at its midpoint, and is identified by the " +
                     "reach's unique NHDPlus COMID identifier.")
    cf_nc.time_coverage_resolution = 'point'
    cf_nc.geospatial_lat_min = 0.0
    cf_nc.geospatial_lat_max = 0.0
    cf_nc.geospatial_lat_units = 'degrees_north'
    cf_nc.geospatial_lat_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_lon_min = 0.0
    cf_nc.geospatial_lon_max = 0.0
    cf_nc.geospatial_lon_units = 'degrees_east'
    cf_nc.geospatial_lon_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_vertical_min = 0.0
    cf_nc.geospatial_vertical_max = 0.0
    cf_nc.geospatial_vertical_units = 'm'
    cf_nc.geospatial_vertical_resolution = 'midpoint of stream feature'
    cf_nc.geospatial_vertical_positive = 'up'
    cf_nc.project = 'National Flood Interoperability Experiment'
    cf_nc.processing_level = 'Raw simulation result'
    cf_nc.keywords_vocabulary = ('NASA/Global Change Master Directory ' +
                                 '(GCMD) Earth Science Keywords. Version ' +
                                 '8.0.0.0.0')
    cf_nc.keywords = 'DISCHARGE/FLOW'
    cf_nc.comment = 'Result time step (seconds): ' + str(time_step_seconds)

    timestamp = datetime.utcnow().isoformat() + 'Z'
    cf_nc.date_created = timestamp
    cf_nc.history = (timestamp + '; added time, lat, lon, z, crs variables; ' +
                     'added metadata to conform to NODC_NetCDF_TimeSeries_' +
                     'Orthogonal_Template_v1.1')
    return cf_nc


def write_comid_lat_lon_z(cf_nc, lookup_filename, id_var_name):
    """Add latitude, longitude, and z values for each netCDF feature

    Arguments:
        cf_nc -- netCDF Dataset object to be modified
        lookup_filename -- full path and filename for lookup table
        id_var_name -- name of Id variable

    Remarks:
        Lookup table is a CSV file with COMID, Lat, Lon, and Elev_m columns.
        Columns must be in that order and these must be the first four columns.
    """

    import csv

    # Get relevant arrays while we update them
    comids = cf_nc.variables[id_var_name][:]
    lats = cf_nc.variables['lat'][:]
    lons = cf_nc.variables['lon'][:]
    zs = cf_nc.variables['z'][:]
    id_count = len(comids)

    lat_min = None
    lat_max = None
    lon_min = None
    lon_max = None
    z_min = None
    z_max = None

    # Process each row in the lookup table
    with open(lookup_filename, 'rb') as csvfile:
        at_header = True
        reader = csv.reader(csvfile)
        index = -1
        for row in reader:
            if at_header:
                at_header = False
            elif index < id_count:
                comids[index] = int(row[0])
                        
                lat = float(row[1])
                lats[index] = lat
                if (lat_min) is None or lat < lat_min:
                    lat_min = lat
                if (lat_max) is None or lat > lat_max:
                    lat_max = lat

                lon = float(row[2])
                lons[index] = lon
                if (lon_min) is None or lon < lon_min:
                    lon_min = lon
                if (lon_max) is None or lon > lon_max:
                    lon_max = lon

                z = float(row[3])
                zs[index] = z
                if (z_min) is None or z < z_min:
                    z_min = z
                if (z_max) is None or z > z_max:
                    z_max = z

            index = index + 1

    # Overwrite netCDF variable values
    cf_nc.variables[id_var_name][:] = comids
    cf_nc.variables['lat'][:] = lats
    cf_nc.variables['lon'][:] = lons
    cf_nc.variables['z'][:] = zs

    if index != id_count:
        msg = 'COMIDs in netCDF: %s. COMIDs in lat-lon-z lookup table: %s'  % (id_count, index)
        log(msg, 'WARNING')

    # Update metadata
    if lat_min is not None:
        cf_nc.geospatial_lat_min = lat_min
    if lat_max is not None:
        cf_nc.geospatial_lat_max = lat_max
    if lon_min is not None:
        cf_nc.geospatial_lon_min = lon_min
    if lon_max is not None:
        cf_nc.geospatial_lon_max = lon_max
    if z_min is not None:
        cf_nc.geospatial_vertical_min = z_min
    if z_max is not None:
        cf_nc.geospatial_vertical_max = z_max

def convert_ecmwf_rapid_output_to_cf_compliant(watershed_name, start_date):
    """Copies data from RAPID netCDF output to a CF-compliant netCDF file.

    Arguments:
        logger -- logger object, if logging is desired
    """

    try:
        # Read config parameters
        config = ConfigParser.RawConfigParser()
        try:
            config.read('cf_config.cfg')
        except:
            log('Failed to read config file', 'ERROR')
            return

        path = get_this_path()

        try:
            time_step = int(config.get('input', 'time_step'))
            input_id_dim_name = config.get('input', 'id_dim')
            input_flow_var_name = config.get('input', 'flow_var')
            output_id_dim_name = config.get('output', 'id_dim')
            output_flow_var_name = config.get('output', 'flow_var')
        except:
            log('Failed to read parameters from config', 'ERROR')
            return

        # Get files to process
        inputs = glob(os.path.join(path),"Qout*.nc")
        if len(inputs) == 0:
            log('No files to process', 'INFO')
            return

        subbasin_name_search = re.compile(r'Qout_(\w+)_\d+.nc')
        for rapid_nc_filename in inputs:

            #make sure comid_lat_lon_z file exists before proceeding
            subbasin_name = subbasin_name_search.search(os.path.basename(rapid_nc_filename)).group(1)
            comid_lat_lon_z_lookup_filename = os.path.join(path,
                                                           watershed_name,
                                                           'comid_lat_lon_z_%s.csv' % subbasin_name)

            if os.path.exists(comid_lat_lon_z_lookup_filename):
                cf_nc_filename = '%s_CF.nc' % os.path.splitext(rapid_nc_filename)[0]
                log('Processing %s' % rapid_nc_filename, 'INFO')
                time_start_conversion = datetime.utcnow()
                # Get dimension size of input file
                rapid_nc = Dataset(rapid_nc_filename)
                time_len = len(rapid_nc.dimensions['Time'])
                id_len = len(rapid_nc.dimensions[input_id_dim_name])

                # Initialize the output file (create dimensions and variables)
                cf_nc = initialize_output(cf_nc_filename, output_id_dim_name,
                                          output_flow_var_name, time_len, id_len,
                                          time_step)

                # Copy flow values. Tranpose to use NODC's dimension order.
                cf_nc.variables[output_flow_var_name][:] = rapid_nc.variables[input_flow_var_name][:].transpose()
                rapid_nc.close()

                # Populate time values
                total_seconds = time_step * time_len
                end_date = (start_date +
                            timedelta(seconds=(total_seconds - time_step)))
                d1970 = datetime(1970, 1, 1)
                secs_start = int((start_date - d1970).total_seconds())
                secs_end = secs_start + total_seconds
                cf_nc.variables['time'][:] = np.arange(
                    secs_start, secs_end, time_step)
                cf_nc.time_coverage_start = start_date.isoformat() + 'Z'
                cf_nc.time_coverage_end = end_date.isoformat() + 'Z'

                # Populate comid, lat, lon, z
                write_comid_lat_lon_z(cf_nc, comid_lat_lon_z_lookup_filename, output_id_dim_name)

                cf_nc.close()

                #delete original RAPID output
                try:
                    os.remove(rapid_nc_filename)
                except OSError:
                    pass

                #replace original with nc compliant file
                shutil.move(cf_nc_filename, rapid_nc_filename)
                log('Time to process %s' % (datetime.utcnow()-time_start_conversion), 'INFO')
            else:
                log("No comid_lat_lon_z found for subbasin %s. Skipping ..." % subbasin_name, "INFO")

        log('Files processed: ' + str(len(inputs)), 'INFO')
    except:
        log('Error in main function', 'ERROR')