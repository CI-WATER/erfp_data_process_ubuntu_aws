#!/bin/sh
/home/sgeadmin/work/scripts/rapid/rapid_process_async_ubuntu.py 1> /mnt/sgeadmin/logs/rapid_$(date +%y%m%d%H%M%S).log 2>&1
