import datetime
import ftplib
from glob import glob
import os
from shutil import rmtree
import tarfile
import time

def ftp_connect():
    """
    Connect to ftp site
    """
    ftp = ftplib.FTP('ftp.ecmwf.int')
    ftp.login('safer','neo2008')
    ftp.cwd('tcyc')
    ftp.set_debuglevel(1)
    return ftp
    
def remove_old_ftp_downloads(folder):
    """
    remove files/folders older than 1 days old
    """
    date_now = datetime.datetime.utcnow()
    all_paths = glob(os.path.join(folder,'Runoff*netcdf*'))
    for path in all_paths:
        date_file = datetime.datetime.strptime(os.path.basename(path).split('.')[1],'%Y%m%d')
        if date_now - date_file > datetime.timedelta(1):
            if os.path.isdir(path):
                rmtree(path)
            else:
                os.remove(path)
                
def download_ftp(dst_filename, local_path):
    """
    Download single file from the ftp site
    """
    file = open(local_path, 'wb')
    print 'Reconnecting ...'
    handle = ftp_connect()
    handle.voidcmd('TYPE I')
    dst_filesize = handle.size(dst_filename)
    attempts_left = 15
    while dst_filesize > file.tell():
        try:
            if file.tell() == 0:
                res = handle.retrbinary('RETR %s' % dst_filename, file.write)
            else:
                # retrieve file from position where we were disconnected
                handle.retrbinary('RETR %s' % dst_filename, file.write, rest=file.tell())
        except Exception as ex:
            print ex
            if attempts_left == 0:
                print "Max number of attempts reached. Download stopped."
                handle.quit()
                file.close()
                os.remove(local_path)
                return False
            print 'Waiting 30 sec...'
            time.sleep(30)
            print 'Reconnecting ...'
            handle.quit()
            handle = ftp_connect()
            print 'Connected. ' + str(attempts_left) + 'attempt(s) left.'
        attempts_left -= 1
    handle.quit()
    file.close()
    return True
    
def download_all_ftp(download_dir, file_match):
    """
    Remove downloads from before 2 days ago
    Download all files from the ftp site matching date
    Extract downloaded files
    """
    remove_old_ftp_downloads(download_dir)
    ftp = ftp_connect()
    #open the file for writing in binary mode
    print 'Opening local file'
    file_list = ftp.nlst(file_match)
    ftp.quit()
    all_files_downloaded = []
    for dst_filename in file_list:
        local_path = os.path.join(download_dir,dst_filename)
        #get correct local_dir
        if local_path.endswith('.tar.gz'):
            local_dir = local_path[:-7]
        else:
            local_dir = download_dir
        #download and unzip file
        try:
            #download from ftp site
            unzip_file = False
            if not os.path.exists(local_path) and not os.path.exists(local_dir):
                print "Downloading from ftp site: " + dst_filename
                unzip_file = download_ftp(dst_filename, local_path)
            else:
                print dst_filename + ' already exists. Skipping download.'
            #extract from tar.gz
            if unzip_file:
                os.mkdir(local_dir)
                print "Extracting: " + dst_filename
                tar = tarfile.open(local_path)
                tar.extractall(local_dir)
                tar.close()
                #add successfully downloaded file to list
                all_files_downloaded.append(local_dir)
            else:
                print dst_filename + ' already extracted. Skipping extraction.'
            #remove the tarfile
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception as ex:
            print ex
            continue
        
    print "All downloads completed!"
    return all_files_downloaded

if __name__ == "__main__":
    ecmwf_forecast_location = "C:/Users/byu_rapid/Documents/RAPID/ECMWF"
    time_string = datetime.datetime.utcnow().strftime('%Y%m%d')
    #time_string = datetime.datetime(2014,11,2).strftime('%Y%m%d')
    all_ecmwf_files = download_all_ftp(ecmwf_forecast_location,'Runoff.'+time_string+'*.netcdf.tar.gz')
