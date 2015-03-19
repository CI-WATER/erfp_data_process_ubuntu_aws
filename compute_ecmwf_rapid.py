#!/usr/bin/python
import csv
import datetime
import fcntl
from glob import glob
import netCDF4 as NET
import numpy as np
import os
from shutil import move
from subprocess import Popen
import sys
from tempfile import mkstemp

sys.path.append("/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws")
from CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff

#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def find_current_rapid_output(path_to_watershed_files, basin_name, current_date):
    """
    Finds the most current files output from RAPID
    """
    #check if there are any from 12 hours ago
    past_date = datetime.datetime.strptime(current_date[:11],"%Y%m%d.%H") - \
        datetime.timedelta(0,12*60*60)
    past_hour = '1200' if past_date.hour > 11 else '0'
    path_to_files = os.path.join(path_to_watershed_files,
        past_date.strftime("%Y%m%d")+'.'+past_hour)
    if os.path.exists(path_to_files):
        basin_files = glob(os.path.join(path_to_files,"*"+basin_name+"*.nc"))
        if len(basin_files) >0:
            return basin_files
    #there are none found
    return None

def compute_initial_rapid_flows(basin_ensemble_files, basin_name, input_directory, output_directory):
    """
    Gets mean of all 52 ensembles 12-hrs ago and prints to csv as initial flow
    Qinit_file (BS_opt_Qinit)
    Qfinal_file (BS_opt_Qfinal)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file,
    and that Qfinal_file (produced when RAPID runs) is ordered the same way as your riv_bas_id_file.
    """
    init_file_location = os.path.join(output_directory,'Qinit_file_'+basin_name+'.csv')
    #check to see if exists and only perform operation once
    if not os.path.exists(init_file_location) and basin_ensemble_files:
        #collect data into matrix
        all_data_series = []
        reach_ids = []
        for in_nc in basin_ensemble_files:
            data_nc = NET.Dataset(in_nc)
            qout = data_nc.variables['Qout']
            #get flow at 12 hr time step for all reaches
            dataValues = qout[2,:].clip(min=0)
            if (len(reach_ids) <= 0):
                reach_ids = data_nc.variables['COMID'][:]
            all_data_series.append(dataValues)
            data_nc.close()
        #get mean of each reach at time step
        mean_data = np.array(np.matrix(all_data_series).mean(0).T)
        #add zeros for reaches not in subbasin
        rapid_connect_file = open(os.path.join(input_directory,'rapid_connect.csv'))
        all_reach_ids = []
        for row in rapid_connect_file:
            all_reach_ids.append(int(row.split(",")[0]))

        #if the reach is not in the subbasin initialize it with zero
        initial_flows = np.array(np.zeros((1,len(all_reach_ids)), dtype='float32').T)
        subbasin_reach_index = 0
        for reach_id in reach_ids:
            new_index = all_reach_ids.index(reach_ids[subbasin_reach_index])
            initial_flows[:][new_index] = mean_data[:][subbasin_reach_index]
            subbasin_reach_index+=1
        #print to csv file
        csv_file = open(init_file_location,"wb")
        writer = csv.writer(csv_file)
        writer.writerows(initial_flows)
        csv_file.close()


def update_namelist_file(namelist_file, rapid_io_files_location, watershed, basin_name,
                ensemble_number, init_flow = False):
    """
    Update RAPID namelist file with new inflow file and output location
    """

    #default duration of 15 days
    duration = 15*24*60*60
    #default interval of 6 hrs
    interval = 6*60*60
    #if it is high res
    if(int(ensemble_number) == 52):
        #duration of 10 days
        duration = 10*24*60*60
        #interval of 3 hrs
        #interval = 3*60*60

    old_file = open(namelist_file)
    fh, abs_path = mkstemp()
    new_file = open(abs_path,'w')
    for line in old_file:
        if line.strip().startswith('BS_opt_Qinit'):
            if (init_flow):
                new_file.write('BS_opt_Qinit       =.true.\n')
            else:
                new_file.write('BS_opt_Qinit       =.false.\n')
        elif line.strip().startswith('Vlat_file'):
            new_file.write('Vlat_file          = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                         'm3_riv_bas_%s.nc' % ensemble_number))
        elif line.strip().startswith('ZS_TauM'):
            new_file.write('ZS_TauM            = ' + str(duration) + '\n')
        elif line.strip().startswith('ZS_dtM'):
            new_file.write('ZS_dtM             = ' + str(86400) + '\n')
        elif line.strip().startswith('ZS_TauR'):
            new_file.write('ZS_TauR            = ' + str(interval) + '\n')
        elif line.strip().startswith('Qinit_file'):
            if (init_flow):
                new_file.write('Qinit_file         = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          'Qinit_file_%s.csv' % basin_name))
            else:
                new_file.write('Qinit_file         = \'\'\n')
        elif line.strip().startswith('rapid_connect_file'):
            new_file.write('rapid_connect_file = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'rapid_connect.csv'))
        elif line.strip().startswith('riv_bas_id_file'):
            new_file.write('riv_bas_id_file    = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'riv_bas_id_%s.csv' % basin_name))
        elif line.strip().startswith('k_file'):
            new_file.write('k_file             = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'k.csv'))
        elif line.strip().startswith('x_file'):
            new_file.write('x_file             = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'x.csv'))
        elif line.strip().startswith('Qout_file'):
            new_file.write('Qout_file          = \'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          'Qout_%s_%s.nc' % (basin_name, ensemble_number)))
        else:
            new_file.write(line)

    #close temp file
    new_file.close()
    os.close(fh)
    old_file.close()
    #Remove original file
    os.remove(namelist_file)
    #Move new file
    move(abs_path, namelist_file)

def run_RAPID_single_watershed(forecast, watershed, rapid_executable_location,
                               node_path):
    """
    run RAPID on single watershed after ECMWF prepared
    """
    forecast_split = os.path.basename(forecast).split(".")
    ensemble_number = int(forecast_split[2])
    input_directory = os.path.join(node_path, watershed)
    rapid_namelist_file = os.path.join(node_path,'rapid_namelist')
    local_rapid_executable = os.path.join(node_path,'rapid')
    #create link to RAPID
    os.symlink(rapid_executable_location, local_rapid_executable)

    #loop through all the rapid_namelist files in directory
    file_list = glob(os.path.join(input_directory,'rapid_namelist_*.dat'))
    for namelist_file in file_list:
        basin_name = os.path.basename(namelist_file)[15:-4]
        #change the new RAPID namelist file
        print "Updating namelist file for: " + basin_name + " " + str(ensemble_number)
        update_namelist_file(namelist_file, node_path,
                watershed, basin_name, ensemble_number)
        #remove link to old RAPID namelist file
        try:
            os.unlink(rapid_namelist_file)
            os.remove(rapid_namelist_file)
        except OSError:
            pass
        #change link to new RAPID namelist file
        os.symlink(namelist_file,rapid_namelist_file)
        #run RAPID
        print "Running RAPID for: %s Ensemble: %s" % (basin_name, ensemble_number)
        process = Popen([local_rapid_executable], shell=True)
        process.communicate()
    #remove rapid link
    try:
        os.unlink(local_rapid_executable)
        os.remove(local_rapid_executable)
    except OSError:
        pass
    #remove namelist link
    try:
        os.unlink(rapid_namelist_file)
        os.remove(rapid_namelist_file)
    except OSError:
        pass

def prepare_all_inflow_ECMWF(ecmwf_forecast, watershed, in_weight_table, rapid_executable_location):
    """
    prepare all ECMWF files for rapid
    """
    node_path = os.path.dirname(os.path.realpath(__file__))
    forecast_basename = os.path.basename(ecmwf_forecast)
    in_weight_table_node_location = os.path.join(node_path, watershed, in_weight_table)
    forecast_split = forecast_basename.split(".")
    forecast_date_timestep = ".".join(forecast_split[:2])
    ensemble_number = int(os.path.basename(ecmwf_forecast).split(".")[2])
    inflow_file_name = 'm3_riv_bas_%s.nc' % ensemble_number

    time_start_all = datetime.datetime.utcnow()
    try:
        #RUN CALCULATIONS
        #prepare ECMWF file for RAPID
        print "Running all ECMWF downscaling for watershed: %s %s %s ..." % (watershed, forecast_date_timestep,
                                                                              ensemble_number)
        print "Converting ECMWF inflow"
        #optional argument ... time interval?
        RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
        RAPIDinflowECMWF_tool.execute(forecast_basename,
            in_weight_table_node_location, inflow_file_name)
        time_finish_ecmwf = datetime.datetime.utcnow()
        print "Time to convert ECMWF: %s" % (time_finish_ecmwf-time_start_all)
        time_start_rapid = datetime.datetime.utcnow()
        run_RAPID_single_watershed(forecast_basename, watershed, rapid_executable_location,
                               node_path)
        time_stop_rapid = datetime.datetime.utcnow()
        print "Time to run RAPID: %s" % (time_stop_rapid-time_start_rapid)
        #CLEAN UP
        print "Cleaning up"
        #remove inflow file
        try:
            os.remove(inflow_file_name)
        except OSError:
            pass
        time_stop_all = datetime.datetime.utcnow()

    except Exception as ex:
        print ex
        print "Skipping ECMWF downscaling for: %s %s %s ..." % (watershed, forecast_date_timestep,
                                                                          ensemble_number)
        return False
    print "Total time to compute: %s" % (time_stop_all-time_start_all)

if __name__ == "__main__":   
    prepare_all_inflow_ECMWF(sys.argv[1],sys.argv[2], sys.argv[3], sys.argv[4])
