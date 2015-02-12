#!/bin/sh
/home/sgeadmin/work/scripts/erfp_data_process_ubuntu_aws/rapid_process_async_ubuntu.py 1> /mnt/sgeadmin/logs/rapid_$(date +%y%m%d%H%M%S).log 2>&1
