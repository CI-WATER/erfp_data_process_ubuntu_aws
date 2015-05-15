# erfp_data_process_ubuntu_aws
Code to use to prepare input data for RAPID from ECMWF forecast using Amazon Web Services (AWS)

##Step 1: Install RAPID
**For Ubuntu:**
```
$ apt-get install gfortran g++
```
Follow the instructions on page 10-14: http://rapid-hub.org/docs/RAPID_Azure.pdf.

##Step 2: Install netCDF4-python
###Install on Ubuntu:
```
$ apt-get install python-dev zlib1g-dev libhdf5-serial-dev libnetcdf-dev 
$ pip install numpy
$ pip install netCDF4
```
###Install on Redhat:
*Note: this tool was desgined and tested in Ubuntu*
```
$ yum install netcdf4-python
$ yum install hdf5-devel
$ yum install netcdf-devel
$ pip install numpy
$ pip install netCDF4
```
##Step 3: Install Other Python Libraries
```
$ pip install requests_toolbelt
$ pip install tethys_dataset_services
$ pip install condorpy
```
##Step 4: Download the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/CI-WATER/erfp_data_process_ubuntu_aws.git
$ git submodule init
$ git submodule update
```
##Step 5: Create folders for RAPID input and for downloading ECMWF
In this instance:
```
$ cd /mnt/sgeadmin/
$ mkdir rapid ecmwf logs condor
$ mkdir rapid/input
```
##Step 6: Change the locations in the files
Go into *rapid_process_async_ubuntu.py* and change these variables for your instance:
```python
    rapid_executable_location = '/home/sgeadmin/work/rapid/src/rapid'
    rapid_io_files_location = '/mnt/sgeadmin/rapid'
    ecmwf_forecast_location = "/mnt/sgeadmin/ecmwf"
    rapid_scripts_location = '/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws'
    data_store_url = 'http://ciwckan.chpc.utah.edu'
    data_store_api_key = 'your-ckan-api-key'
    condor_init_dir = "/mnt/sgeadmin/condor/%s" % date_string
```
Go into *rapid_process.sh* and change make sure the path locations and variables are correct for your instance.
Go into *compute_ecmwf_rapid.py* and make sure the path location is correct:
```python
sys.path.append("/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws")
```

##Step 7: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod 554 rapid_process_async_ubuntu.py
$ chmod 554 rapid_process.sh
```
##Step 8: Add RAPID files to the work/rapid/input directory
Example:
```
$ ls /rapid/input
huc_region_1209
$ ls -lh /rapid/input/huc_region_1209
-r--r--r-- 1 alan alan 163K Mar  6 10:01 k.csv
-r--r--r-- 1 alan alan 163K Mar  6 10:01 kfac.csv
-r--r--r-- 1 alan alan 340K Mar  6 19:22 rapid_connect.csv
-rw-r--r-- 1 alan alan 5.1K Mar 25 04:15 rapid_namelist_huc_4_1209.dat
-r--r--r-- 1 alan alan  99K Mar  9 07:52 riv_bas_id_huc_4_1209.csv
-r--r--r-- 1 alan alan 1.5M Mar  9 08:03 weight_high_res.csv
-r--r--r-- 1 alan alan 1.2M Mar  9 08:03 weight_low_res.csv
-r--r--r-- 1 alan alan  55K Mar  6 10:01 x.csv
```
##Step 9: Create CRON job to run the scripts twice daily
See: http://askubuntu.com/questions/2368/how-do-i-set-up-a-cron-job

You only need to run rapid_process.sh
```
$ ./rapid_process.sh
```
You can use *create_cron.py* to create the CRON jobs.

1) Install crontab Python package.
```
$ pip install python-crontab
```
2) Modify location of script
```python
cron_command = '/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws/rapid_process.sh' 
```
3) Change execution times to suit your needs
```python
cron_job_morning.minute.on(30)
cron_job_morning.hour.on(9)
...
cron_job_evening.minute.on(30)
cron_job_evening.hour.on(21)
```

