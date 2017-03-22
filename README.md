# foop
This Project is built to FTP the files from the FTPServer to Hadoop. 

Purpose of this project as below:
1. Pull data from any FTP server. Ex: Mainframe, FTP, etc
2. Multiple files from FTP server needs to be downloaded in parallel 
3. Files should directly pushed to HDFS

Command Syntax:
Foop --connection {ftp_url} --username {userid} --password {password} --source-dir {source file/dir} --target-dir {target hdfs dir} [--ftp-type-ascii|--ftp-type-bin-rdw]
