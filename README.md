# erfp_data_process_ubuntu_aws
Code to use to prepare input data for RAPID from ECMWF forecast using HTCondor

Note: For steps 1-2, use the *install_rapid_htcondor.sh* at your own risk.

##Step 1: Install RAPID
**For Ubuntu:**
```
$ apt-get install gfortran g++
```
Follow the instructions on page 10-14: http://rapid-hub.org/docs/RAPID_Azure.pdf.

Here is a script to download prereqs: http://rapid-hub.org/data/rapid_install_prereqs.sh.gz

##Step 2: Install HTCondor (if not using Amazon Web Services and StarCluster)
```
apt-get install -y libvirt0 libdate-manip-perl vim
wget http://ciwckan.chpc.utah.edu/dataset/be272798-f2a7-4b27-9dc8-4a131f0bb3f0/resource/86aa16c9-0575-44f7-a143-a050cd72f4c8/download/condor8.2.8312769ubuntu14.04amd64.deb
dpkg -i condor8.2.8312769ubuntu14.04amd64.deb
#if master node uncomment CONDOR_HOST and comment out CONDOR_HOST and DAEMON_LIST lines
#echo CONDOR_HOST = \$\(IP_ADDRESS\)
echo CONDOR_HOST = 10.8.123.71 >> /etc/condor/condor_config.local
echo DAEMON_LIST = MASTER, SCHEDD, STARTD >> /etc/condor/condor_config.local
echo ALLOW_ADMINISTRATOR = \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_OWNER = \$\(FULL_HOSTNAME\), \$\(ALLOW_ADMINISTRATOR\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_READ = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo ALLOW_WRITE = \$\(FULL_HOSTNAME\), \$\(CONDOR_HOST\), 10.8.123.* >> /etc/condor/condor_config.local
echo START = True >> /etc/condor/condor_config.local
echo SUSPEND = False >> /etc/condor/condor_config.local
echo CONTINUE = True >> /etc/condor/condor_config.local
echo PREEMPT = False >> /etc/condor/condor_config.local
echo KILL = False >> /etc/condor/condor_config.local
echo WANT_SUSPEND = False >> /etc/condor/condor_config.local
echo WANT_VACATE = False >> /etc/condor/condor_config.local
. /etc/init.d/condor start
```
NOTE: if you forgot to change lines for master node, change CONDOR_HOST = $(IP_ADDRESS)
and run $ . /etc/init.d/condor restart as ROOT
##Step 3: Install netCDF4-python
###Install on Ubuntu:
```
$ apt-get install python-dev zlib1g-dev libhdf5-serial-dev libnetcdf-dev 
$ pip install numpy netCDF4
```
###Install on Redhat:
*Note: this tool was desgined and tested in Ubuntu*
```
$ yum install netcdf4-python
$ yum install hdf5-devel
$ yum install netcdf-devel
$ pip install numpy netCDF4
```
##Step 4: Install Other Python Libraries
```
$ pip install requests_toolbelt tethys_dataset_services condorpy
```
##Step 5: Download the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/CI-WATER/erfp_data_process_ubuntu_aws.git
$ cd erfp_data_process_ubuntu_aws
$ git submodule init
$ git submodule update
```
##Step 6: Create folders for RAPID input and for downloading ECMWF
In this instance:
```
$ cd /mnt/sgeadmin/
$ mkdir rapid ecmwf logs condor
$ mkdir rapid/input
```
##Step 7: Change the locations in the files
Go into *rapid_process_async_ubuntu.py* and change these variables for your instance:
```python
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_ecmwf_rapid_process(
        rapid_executable_location='/home/cecsr/work/rapid/src/rapid',
        rapid_io_files_location='/home/cecsr/rapid',
        ecmwf_forecast_location ="/home/cecsr/ecmwf",
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
```
Go into *rapid_process.sh* and change make sure the path locations and variables are correct for your instance.

Go into *ftp_ecmwf_download.py* and add password and login information:
```python
    #init FTPClient
    ftp_client = PyFTPclient(host='ftp.ecmwf.int',
                             login='',
                             passwd='',
                             directory='tcyc')
```

##Step 8: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod 554 rapid_process_async_ubuntu.py
$ chmod 554 rapid_process.sh
```
##Step 9: Add RAPID files to the work/rapid/input directory
Make sure the directory is in the format [watershed name]-[subbasin name]
with lowercase letters, numbers, and underscores only. No spaces!


Example:
```
$ ls /rapid/input
nfie_texas_gulf_region-huc_2_12
$ ls /rapid/input/nfie_texas_gulf_region-huc_2_12
k.csv
rapid_connect.csv
riv_bas_id.csv
weight_high_res.csv
weight_low_res.csv
x.csv
```
##Step 10: Create CRON job to run the scripts twice daily
See: http://askubuntu.com/questions/2368/how-do-i-set-up-a-cron-job

You only need to run rapid_process.sh
```
$ ./rapid_process.sh
```
###How to use *create_cron.py* to create the CRON jobs:

1) Install crontab Python package.
```
$ pip install python-crontab
```
2) Modify location of script in *create_cron.py*
```python
cron_command = '/home/cecsr/scripts/erfp_data_process_ubuntu_aws/rapid_process.sh'
```
3) Change execution times to suit your needs in *create_cron.py*
```python
cron_job_morning.minute.on(30)
cron_job_morning.hour.on(9)
...
cron_job_evening.minute.on(30)
cron_job_evening.hour.on(21)
```

#Troubleshooting
If you see this error:
ImportError: No module named packages.urllib3.poolmanager
```
$ pip install pip --upgrade
```
Restart your terminal
```
$ pip install requests --upgrade
```
