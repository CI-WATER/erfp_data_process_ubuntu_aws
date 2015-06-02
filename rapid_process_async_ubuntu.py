#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
from condorpy import Node, DAG
import datetime
from glob import glob
import itertools
from math import ceil
import os
from shutil import rmtree
import subprocess

#local imports
import ftp_ecmwf_download
from sfpt_dataset_manager.dataset_manager import RAPIDInputDatasetManager

#----------------------------------------------------------------------------------------
# FUNCTIONS
#----------------------------------------------------------------------------------------
def get_node_names(start_index, num_nodes):
    """
    This function generates names of the nodes to add to the cluster
    """
    node_names = ""
    for i in range(start_index, start_index+num_nodes):
        node_names += "node%s," %i
    return node_names[:-1]

def main():
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()
    date_string = time_begin_all.strftime('%Y%m%d')
    #date_string = datetime.datetime(2015,2,3).strftime('%Y%m%d')

    rapid_executable_location = '/home/sgeadmin/work/rapid/src/rapid'
    rapid_io_files_location = '/rapid'
    ecmwf_forecast_location = "/rapid/ecmwf"
    rapid_scripts_location = '/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws'
    condor_directory = '/mnt/sgeadmin/condor/'
    data_store_url = 'http://ciwckan.chpc.utah.edu'
    data_store_api_key = '8dcc1b34-0e09-4ddc-8356-df4a24e5be87'
    app_instance_id = '53ab91374b7155b0a64f0efcd706854e'
    sync_with_ckan = True
    download_ecmwf = True
    upload_to_ckan = True

    """
    cluster_name = "rapid"
    node_image_id = "ami-b4ab14c3"
    num_nodes_per_watershed = 26
    """
    if sync_with_ckan and app_instance_id and data_store_url and data_store_api_key:
        #sync with data store
        ri_manager = RAPIDInputDatasetManager(data_store_url,
                                              data_store_api_key,
                                              'ecmwf',
                                              app_instance_id)
        ri_manager.sync_dataset(os.path.join(rapid_io_files_location,'input'))

    #initialize HTCondor Directory
    condor_init_dir = os.path.join(condor_directory, date_string)
    try:
        os.makedirs(condor_init_dir)
    except OSError:
        pass

    #get list of watersheds in rapid directory
    watersheds = [d for d in os.listdir(os.path.join(rapid_io_files_location,'input')) \
                if os.path.isdir(os.path.join(rapid_io_files_location,'input', d))]


    """
    #add nodes to cluster
    tot_num_nodes = float(len(watersheds)*num_nodes_per_watershed-1)

    #c3.large (2)
    num_add_large_nodes = min(3, int(ceil(tot_num_nodes/2)))
    if num_add_large_nodes>0:
        add_large_node_args = ["starcluster","addnode","-i", node_image_id, "-I", "c3.large", "-n", str(num_add_large_nodes),
                               "-a", get_node_names(0,num_add_large_nodes), cluster_name]

        add_large_node_process = subprocess.Popen(add_large_node_args)
        print "Adding %s c3.large node(s) to the cluster..." % num_add_large_nodes
    """
    if download_ecmwf:
        #download all files for today
        ecmwf_folders = ftp_ecmwf_download.download_all_ftp(ecmwf_forecast_location,
           'Runoff.%s*.netcdf.tar.gz' % date_string)
    else:
        ecmwf_folders = glob(os.path.join(ecmwf_forecast_location,
            'Runoff.'+date_string+'*.netcdf'))
    """
    #continue adding nodes
    #c3.xlarge (4)
    num_add_xlarge_nodes = min(4, int(ceil((tot_num_nodes-num_add_large_nodes*2)/4)))
    if num_add_xlarge_nodes>0:
        add_large_node_process.communicate()
        add_xlarge_node_args = ["starcluster","addnode","-i", node_image_id, "-I", "c3.xlarge", "-n", str(num_add_xlarge_nodes),
                               "-a", get_node_names(4,num_add_xlarge_nodes), cluster_name]
        add_xlarge_node_process = subprocess.Popen(add_xlarge_node_args)
        print "Adding %s c3.xlarge node(s) to the cluster..." % num_add_xlarge_nodes

    #cl.2xlarge (8)
    num_add_2xlarge_nodes = min(4, int(ceil((tot_num_nodes-num_add_large_nodes*2-num_add_xlarge_nodes*4)/8)))
    if num_add_2xlarge_nodes>0:
        add_xlarge_node_process.communicate()
        add_2xlarge_node_args = ["starcluster","addnode","-i", node_image_id, "-I", "c3.2xlarge", "-n", str(num_add_2xlarge_nodes),
                               "-a", get_node_names(8,num_add_2xlarge_nodes), cluster_name]
        add_2xlarge_node_process = subprocess.Popen(add_2xlarge_node_args)
        print "Adding %s c3.2xlarge node(s) to the cluster..." % num_add_2xlarge_nodes

    if tot_num_nodes < (num_add_large_nodes*2+num_add_xlarge_nodes*4+num_add_2xlarge_nodes*8):
        print "Max number of nodes reached, no more will be added."
    """

    #prepare ECMWF files
    time_start_prepare = datetime.datetime.utcnow()
    ecmwf_forecasts = []
    for ecmwf_folder in ecmwf_folders:
        ecmwf_forecasts += glob(os.path.join(ecmwf_folder,'*.runoff.netcdf'))
    #make the largest files first
    ecmwf_forecasts.sort(key=os.path.getsize, reverse=True)

    #submit jobs to downsize ecmwf files to watershed
    iteration = 0
    job_list = []
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
        for namelist_file in file_list:
            basin_name = os.path.basename(namelist_file)[15:-4]
            outflow_file_name = 'Qout_%s_%s.nc' % (basin_name, ensemble_number)
            node_rapid_outflow_file = outflow_file_name
            master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)
            transfer_output_remaps += "%s = %s;" % (node_rapid_outflow_file, master_rapid_outflow_file)

        #determine weight table from resolution
        weight_table_file = 'weight_low_res.csv'
        if ensemble_number == 52:
            weight_table_file = 'weight_high_res.csv'

        #create job to downscale forecasts for watershed
        job = CJob('job_%s_%s_%s' % (forecast_date_timestep, watershed, iteration), tmplt.vanilla_transfer_files)
        job.set('executable',os.path.join(rapid_scripts_location,'compute_ecmwf_rapid.py'))
        job.set('transfer_input_files', "%s, %s, %s" % (forecast, master_watershed_input_directory, rapid_scripts_location))
        job.set('initialdir',condor_init_dir)
        job.set('arguments', '%s %s %s %s %s %s' % (forecast, watershed, weight_table_file, rapid_executable_location,
                                                    data_store_url, data_store_api_key))
        job.set('transfer_output_remaps',"\"%s\"" % (transfer_output_remaps[:-1]))
        job.submit()
        job_list.append(job)
        iteration += 1

    #wait for jobs to finish
    for job in job_list:
        job.wait()

    time_finish_prepare = datetime.datetime.utcnow()

    """
    if upload_to_ckan and data_store_url and data_store_api_key:
        #delete local datasets
        for item in os.listdir(os.path.join(rapid_io_files_location, 'output')):
            rmtree(os.path.join(rapid_io_files_location, 'output', item))
    """
    """
    #remove all nodes
    subprocess.Popen(["starcluster","-r","eu-west-1","removenode","-n",
                      str(num_add_large_nodes+num_add_xlarge_nodes+num_add_2xlarge_nodes),"-f","-c",cluster_name])
    """

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
    main()
