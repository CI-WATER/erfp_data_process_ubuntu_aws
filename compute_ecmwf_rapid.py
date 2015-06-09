#!/usr/bin/python
import datetime
from glob import glob
import os
from shutil import move
from subprocess import Popen
import sys
from tempfile import mkstemp

from erfp_data_process_ubuntu_aws.CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from erfp_data_process_ubuntu_aws.make_CF_RAPID_output import convert_ecmwf_rapid_output_to_cf_compliant
#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def update_namelist_file(namelist_file, rapid_io_files_location, watershed, basin_name,
                         ensemble_number, forecast_date_timestep, init_flow = False):
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

    qinit_file = None
    if(init_flow):
        #check for qinit file
        past_date = (datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H") - \
                     datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")
        qinit_file = os.path.join(rapid_io_files_location,
                                  'Qinit_file_%s_%s.csv' % (basin_name, past_date))
        init_flow = qinit_file and os.path.exists(qinit_file)

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
            new_file.write('Vlat_file          =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                         'm3_riv_bas_%s.nc' % ensemble_number))
        elif line.strip().startswith('ZS_TauM'):
            new_file.write('ZS_TauM            =%s\n' % duration)
        elif line.strip().startswith('ZS_dtM'):
            new_file.write('ZS_dtM             =%s\n' % 86400)
        elif line.strip().startswith('ZS_TauR'):
            new_file.write('ZS_TauR            =%s\n' % interval)
        elif line.strip().startswith('Qinit_file'):
            if (init_flow):
                new_file.write('Qinit_file         =\'%s\'\n' % qinit_file)
            else:
                new_file.write('Qinit_file         =\'\'\n')
        elif line.strip().startswith('rapid_connect_file'):
            new_file.write('rapid_connect_file =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'rapid_connect.csv'))
        elif line.strip().startswith('riv_bas_id_file'):
            new_file.write('riv_bas_id_file    =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'riv_bas_id_%s.csv' % basin_name))
        elif line.strip().startswith('k_file'):
            new_file.write('k_file             =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'k.csv'))
        elif line.strip().startswith('x_file'):
            new_file.write('x_file             =\'%s\'\n' % os.path.join(rapid_io_files_location,
                                                                          watershed,
                                                                          'x.csv'))
        elif line.strip().startswith('Qout_file'):
            new_file.write('Qout_file          =\'%s\'\n' % os.path.join(rapid_io_files_location,
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

def run_RAPID_single_watershed(forecast, watershed, rapid_executable_location, node_path, init_flow):
    """
    run RAPID on single watershed after ECMWF prepared
    """
    forecast_split = os.path.basename(forecast).split(".")
    ensemble_number = int(forecast_split[2])
    forecast_date_timestep = ".".join(forecast_split[:2])
    input_directory = os.path.join(node_path, watershed)
    rapid_namelist_file = os.path.join(node_path,'rapid_namelist')
    local_rapid_executable = os.path.join(node_path,'rapid')
    #create link to RAPID
    os.symlink(rapid_executable_location, local_rapid_executable)

    #loop through all the rapid_namelist files in directory
    file_list = glob(os.path.join(input_directory,'rapid_namelist_*.dat'))
    for namelist_file in file_list:
        time_start_rapid = datetime.datetime.utcnow()

        basin_name = os.path.basename(namelist_file)[15:-4]
        #change the new RAPID namelist file
        print "Updating namelist file for:", basin_name, ensemble_number
        update_namelist_file(namelist_file, node_path,
                             watershed, basin_name,
                             ensemble_number, forecast_date_timestep,
                             init_flow)
        #remove link to old RAPID namelist file
        try:
            os.unlink(rapid_namelist_file)
            os.remove(rapid_namelist_file)
        except OSError:
            pass
        #change link to new RAPID namelist file
        os.symlink(namelist_file,rapid_namelist_file)
        #run RAPID
        print "Running RAPID for:", basin_name, "Ensemble:", ensemble_number
        process = Popen([local_rapid_executable], shell=True)
        process.communicate()

        print "Time to run RAPID:",(datetime.datetime.utcnow()-time_start_rapid)


    #convert rapid output to be CF compliant
    convert_ecmwf_rapid_output_to_cf_compliant(watershed,
                                               datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"))

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

def process_upload_ECMWF_RAPID(ecmwf_forecast, watershed, in_weight_table, rapid_executable_location, init_flow):
    """
    prepare all ECMWF files for rapid
    """
    node_path = os.path.dirname(os.path.realpath(__file__))
    forecast_basename = os.path.basename(ecmwf_forecast)
    in_weight_table_node_location = os.path.join(node_path, watershed, in_weight_table)
    forecast_split = forecast_basename.split(".")
    forecast_date_timestep = ".".join(forecast_split[:2])
    ensemble_number = int(forecast_split[2])
    inflow_file_name = 'm3_riv_bas_%s.nc' % ensemble_number

    time_start_all = datetime.datetime.utcnow()
    try:
        #RUN CALCULATIONS
        #prepare ECMWF file for RAPID
        print "Running all ECMWF downscaling for watershed:", watershed, forecast_date_timestep, ensemble_number
        print "Converting ECMWF inflow"
        #optional argument ... time interval?
        RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
        RAPIDinflowECMWF_tool.execute(forecast_basename,
            in_weight_table_node_location, inflow_file_name)
        time_finish_ecmwf = datetime.datetime.utcnow()
        print "Time to convert ECMWF: %s" % (time_finish_ecmwf-time_start_all)
        run_RAPID_single_watershed(forecast_basename, watershed, rapid_executable_location, node_path, init_flow)
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
        print "Skipping ECMWF-RAPID downscaling for: %s %s %s ..." % (watershed, forecast_date_timestep,
                                                                          ensemble_number)
        return False
    print "Total time to compute: %s" % (time_stop_all-time_start_all)

if __name__ == "__main__":   
    process_upload_ECMWF_RAPID(sys.argv[1],sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])