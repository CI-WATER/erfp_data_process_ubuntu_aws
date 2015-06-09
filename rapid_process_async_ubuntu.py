#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
import csv
import datetime
from glob import glob
import itertools
import netCDF4 as NET
import numpy as np
import os
import re
from shutil import rmtree
import tarfile

#local imports
import ftp_ecmwf_download
from sfpt_dataset_manager.dataset_manager import (ECMWFRAPIDDatasetManager,
                                                  RAPIDInputDatasetManager)

#----------------------------------------------------------------------------------------
# FUNCTIONS
#----------------------------------------------------------------------------------------
def clean_logs(condor_log_directory, main_log_directory):
    """
    This removed logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)
    #clean up condor logs
    condor_dirs = [d for d in os.listdir(condor_log_directory) if os.path.isdir(os.path.join(condor_log_directory, d))]
    for condor_dir in condor_dirs:
        dir_datetime = datetime.datetime.strptime(condor_dir, "%Y%m%d")
        if (date_today-dir_datetime > week_timedelta):
            rmtree(os.path.join(condor_log_directory, condor_dir))

    #clean up log files
    main_log_files = [f for f in os.listdir(main_log_directory) if not os.path.isdir(os.path.join(main_log_directory, f))]
    for main_log_file in main_log_files:
        log_datetime = datetime.datetime.strptime(main_log_file, "rapid_%y%m%d%H%M%S.log")
        if (date_today-log_datetime > week_timedelta):
            os.remove(os.path.join(main_log_directory, main_log_file))

def find_current_rapid_output(forecast_directory, basin_name):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(os.path.join(forecast_directory,"Qout_%s_*.nc" % basin_name))
        if len(basin_files) >0:
            return basin_files
    #there are none found
    return None

def csv_to_list(csv_file, delimiter=','):
    """
    Reads in a CSV file and returns the contents as list,
    where every row is stored as a sublist, and each element
    in the sublist represents 1 cell in the table.

    """
    with open(csv_file, 'rb') as csv_con:
        reader = csv.reader(csv_con, delimiter=delimiter)
        return list(reader)

def get_comids_in_netcdf_file(reach_id_list, prediction_file):
    """
    Gets the subset comid_index_list, reordered_comid_list from the netcdf file
    """
    data_nc = NET.Dataset(prediction_file, mode="r")
    com_ids = data_nc.variables['COMID'][:]
    data_nc.close()
    try:
        #get where comids are in netcdf file
        netcdf_reach_indices_list = np.where(np.in1d(com_ids, reach_id_list))[0]
    except Exception as ex:
        print ex

    return netcdf_reach_indices_list, com_ids[netcdf_reach_indices_list]

def compute_initial_rapid_flows(prediction_files, basin_name, input_directory, forecast_date_timestep):
    """
    Gets mean of all 52 ensembles 12-hrs in future and prints to csv as initial flow
    Qinit_file (BS_opt_Qinit)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file
    if subset of list, add zero where there is no flow
    """
    #remove old init files for this basin
    past_init_flow_files = glob(os.path.join(input_directory, 'Qinit_file_%s_*.csv' % basin_name))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H").strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory,'Qinit_file_%s_%s.csv' % (basin_name, current_forecast_date))
    #check to see if exists and only perform operation once
    if prediction_files:
        #get list of COMIDS
        connectivity_file = csv_to_list(os.path.join(input_directory,'rapid_connect.csv'))
        comid_list = np.array([int(row[0]) for row in connectivity_file])


        print "Finding COMID indices ..."
        comid_index_list, reordered_comid_list = get_comids_in_netcdf_file(comid_list, prediction_files[0])
        print "Extracting data ..."
        reach_prediciton_array = np.zeros((len(comid_list),len(prediction_files),1))
        #get information from datasets
        for file_index, prediction_file in enumerate(prediction_files):
            try:
                #Get hydrograph data from ECMWF Ensemble
                data_nc = NET.Dataset(prediction_file, mode="r")
                qout_dimensions = data_nc.variables['Qout'].dimensions
                if qout_dimensions[0] == 'Time' and qout_dimensions[1] == 'COMID':
                    data_values_2d_array = data_nc.variables['Qout'][2,comid_index_list].transpose()
                    for comid_index, comid in enumerate(reordered_comid_list):
                        reach_prediciton_array[comid_index][file_index] = data_values_2d_array[comid_index]
                else:
                    print "Invalid ECMWF forecast file", prediction_file
                    data_nc.close()
                    continue
                data_nc.close()

            except Exception, e:
                print e
                #pass

        print "Analyzing data ..."
        output_data = []
        for comid in comid_list:
            try:
                #get where comids are in netcdf file
                comid_index = np.where(reordered_comid_list==comid)[0][0]
            except Exception as ex:
                #comid not found in list. Adding zero init flow ...
                output_data.append([0])
                pass
                continue

            #get mean of series as init flow
            output_data.append([np.mean(reach_prediciton_array[comid_index])])

        print "Writing output ..."
        with open(init_file_location, 'wb') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(output_data)
    else:
        print "No current forecasts found. Skipping ..."

def run_ecmwf_rapid_process(rapid_executable_location, rapid_io_files_location, ecmwf_forecast_location,
                            rapid_scripts_location, condor_log_directory, main_log_directory, data_store_url,
                            data_store_api_key, app_instance_id, sync_rapid_input_with_ckan, download_ecmwf,
                            upload_output_to_ckan, initialize_flows):
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()
    date_string = time_begin_all.strftime('%Y%m%d')
    #date_string = datetime.datetime(2015,2,3).strftime('%Y%m%d')

    if sync_rapid_input_with_ckan and app_instance_id and data_store_url and data_store_api_key:
        #sync with data store
        ri_manager = RAPIDInputDatasetManager(data_store_url,
                                              data_store_api_key,
                                              'ecmwf',
                                              app_instance_id)
        ri_manager.sync_dataset(os.path.join(rapid_io_files_location,'input'))

    #clean up old log files
    clean_logs(condor_log_directory, main_log_directory)

    #initialize HTCondor Directory
    condor_init_dir = os.path.join(condor_log_directory, date_string)
    try:
        os.makedirs(condor_init_dir)
    except OSError:
        pass

    #get list of watersheds in rapid directory
    watersheds = [d for d in os.listdir(os.path.join(rapid_io_files_location,'input')) \
                if os.path.isdir(os.path.join(rapid_io_files_location,'input', d))]

    if download_ecmwf:
        #download all files for today
        ecmwf_folders = ftp_ecmwf_download.download_all_ftp(ecmwf_forecast_location,
           'Runoff.%s*.netcdf.tar.gz' % date_string)
    else:
        ecmwf_folders = glob(os.path.join(ecmwf_forecast_location,
            'Runoff.'+date_string+'*.netcdf'))

    if upload_output_to_ckan and data_store_url and data_store_api_key:
        #init data manager for CKAN
        data_manager = ECMWFRAPIDDatasetManager(data_store_url,
                                                data_store_api_key)
        subbasin_name_search = re.compile(r'Qout_(\w+)_\d+.nc')

    #prepare ECMWF files
    time_start_prepare = datetime.datetime.utcnow()

    for ecmwf_folder in ecmwf_folders:
        ecmwf_forecasts = glob(os.path.join(ecmwf_folder,'*.runoff.netcdf'))
        #make the largest files first
        ecmwf_forecasts.sort(key=os.path.getsize, reverse=True)

        #submit jobs to downsize ecmwf files to watershed
        iteration = 0
        job_list = []
        job_info_list = []
        for forecast_combo in itertools.product(ecmwf_forecasts, watersheds):
            forecast = forecast_combo[0]
            watershed = forecast_combo[1]
            forecast_split = os.path.basename(forecast).split(".")
            forecast_date_timestep = ".".join(forecast_split[:2])
            ensemble_number = int(forecast_split[2])
            master_watershed_input_directory = os.path.join(rapid_io_files_location, "input", watershed)
            master_watershed_outflow_directory = os.path.join(rapid_io_files_location, 'output',
                                                   watershed, forecast_date_timestep)
            try:
                os.makedirs(master_watershed_outflow_directory)
            except OSError:
                pass
            #get basin names
            file_list = glob(os.path.join(master_watershed_input_directory,'rapid_namelist_*.dat'))
            transfer_output_remaps = ""
            outflow_file_name_list = []
            for namelist_file in file_list:
                basin_name = os.path.basename(namelist_file)[15:-4]
                outflow_file_name = 'Qout_%s_%s.nc' % (basin_name, ensemble_number)
                node_rapid_outflow_file = outflow_file_name
                master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)
                transfer_output_remaps += "%s = %s;" % (node_rapid_outflow_file, master_rapid_outflow_file)
                outflow_file_name_list.append(master_rapid_outflow_file)

            #determine weight table from resolution
            weight_table_file = 'weight_low_res.csv'
            if ensemble_number == 52:
                weight_table_file = 'weight_high_res.csv'

            #create job to downscale forecasts for watershed
            job = CJob('job_%s_%s_%s' % (forecast_date_timestep, watershed, iteration), tmplt.vanilla_transfer_files)
            job.set('executable',os.path.join(rapid_scripts_location,'compute_ecmwf_rapid.py'))
            job.set('transfer_input_files', "%s, %s, %s" % (forecast, master_watershed_input_directory, rapid_scripts_location))
            job.set('initialdir',condor_init_dir)
            job.set('arguments', '%s %s %s %s %s' % (forecast, watershed, weight_table_file,
                                                  rapid_executable_location, initialize_flows))
            job.set('transfer_output_remaps',"\"%s\"" % (transfer_output_remaps[:-1]))
            job.submit()
            job_list.append(job)
            job_info_list.append({'watershed' : watershed,
                                  'outflow_file_name_list' : outflow_file_name_list,
                                  'forecast_date_timestep' : forecast_date_timestep,
                                  'ensemble_number': ensemble_number,
                                  'master_watershed_outflow_directory': master_watershed_outflow_directory,
                                  })
            iteration += 1

        #wait for jobs to finish then upload files
        for index, job in enumerate(job_list):
            job.wait()
            #upload file when done
            if upload_output_to_ckan and data_store_url and data_store_api_key:
                job_info = job_info_list[index]
                print "Uploading", job_info['watershed'], job_info['forecast_date_timestep'], job_info['ensemble_number']
                for outflow_file in job_info['outflow_file_name_list']:
                    basin_name = subbasin_name_search.search(os.path.basename(outflow_file)).group(1)
                    #Upload to CKAN
                    data_manager.initialize_run_ecmwf(job_info['watershed'], basin_name, job_info['forecast_date_timestep'])
                    data_manager.update_resource_ensemble_number(job_info['ensemble_number'])
                    #upload file
                    try:
                        #tar.gz file
                        output_tar_file =  os.path.join(job_info['master_watershed_outflow_directory'], "%s.tar.gz" % data_manager.resource_name)
                        if not os.path.exists(output_tar_file):
                            with tarfile.open(output_tar_file, "w:gz") as tar:
                                tar.add(outflow_file, arcname=os.path.basename(outflow_file))
                        return_data = data_manager.upload_resource(output_tar_file)
                        if not return_data['success']:
                            print return_data
                            print "Attempting to upload again"
                            return_data = data_manager.upload_resource(output_tar_file)
                            if not return_data['success']:
                                print return_data
                            else:
                                print "Upload success"
                        else:
                            print "Upload success"
                    except Exception, e:
                        print e
                        pass
                    #remove tar.gz file
                    os.remove(output_tar_file)

        time_finish_prepare = datetime.datetime.utcnow()

        #initialize flows for next run
        if initialize_flows:
            #create new init flow files
            for watershed in watersheds:
                input_directory = os.path.join(rapid_io_files_location, 'input', watershed)
                path_to_watershed_files = os.path.join(rapid_io_files_location, 'output', watershed)
                forecast_date_timestep = None
                #finds the current output from downscaled ECMWF forecasts
                if os.path.exists(path_to_watershed_files):
                    forecast_date_timestep = sorted([d for d in os.listdir(path_to_watershed_files) \
                                        if os.path.isdir(os.path.join(path_to_watershed_files, d))],
                                         reverse=True)[0]

                if forecast_date_timestep:
                    #loop through all the rapid_namelist files in directory
                    file_list = glob(os.path.join(input_directory,'rapid_namelist_*.dat'))
                    forecast_directory = os.path.join(path_to_watershed_files, forecast_date_timestep)
                    for namelist_file in file_list:
                        basin_name = os.path.basename(namelist_file)[15:-4]
                        print "Initializing flows for", watershed, basin_name, "from", forecast_date_timestep
                        basin_files = find_current_rapid_output(forecast_directory, basin_name)
                        compute_initial_rapid_flows(basin_files, basin_name,
                                                    input_directory, forecast_date_timestep)


        if upload_output_to_ckan and data_store_url and data_store_api_key:
            #delete local datasets
            for job_info in job_info_list:
                try:
                    rmtree(job_info['master_watershed_outflow_directory'])
                except OSError:
                    pass
            #delete watershed folder if empty
            for item in os.listdir(os.path.join(rapid_io_files_location, 'output')):
                try:
                    os.rmdir(os.path.join(rapid_io_files_location, 'output', item))
                except OSError:
                    pass

    #print info to user
    time_end = datetime.datetime.utcnow()
    print "Time Begin All: " + str(time_begin_all)
    print "Time to Download: " + str(time_start_prepare - time_begin_all)
    print "Time Start Prepare: " + str(time_start_prepare)
    print "Time to Prepare Forecasts: " + str(time_finish_prepare-time_start_prepare)
    print "Time Finish All: " + str(time_end)
    print "TOTAL TIME: "  + str(time_end-time_begin_all)

#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_ecmwf_rapid_process(
        rapid_executable_location='/home/cecsr/work/rapid/src/rapid',
        rapid_io_files_location='/home/cecsr/rapid',
        ecmwf_forecast_location ="/home/cecsr/ecmwf",
        rapid_scripts_location='/home/cecsr/scripts/erfp_data_process_ubuntu_aws',
        condor_log_directory='/home/cecsr/condor/',
        main_log_directory='/home/cecsr/logs/',
        data_store_url='http://ciwckan.chpc.utah.edu',
        data_store_api_key='8dcc1b34-0e09-4ddc-8356-df4a24e5be87',
        app_instance_id='53ab91374b7155b0a64f0efcd706854e',
        sync_rapid_input_with_ckan=False,
        download_ecmwf=True,
        upload_output_to_ckan=True,
        initialize_flows=True
    )
