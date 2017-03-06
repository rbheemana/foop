# foop
This Project is built to FTP the files from the FTPServer to Hadoop. Original source is pulled from CobolSerde project to make the FTP more generic for anytype of server. So it should work perfectly fine for Mainframe files

Command Syntax:
Foop --connection {ftp_url} --username {userid} --password {password} --source-dir {source file/dir} --target-dir {target hdfs dir} [--ftp-type-ascii|--ftp-type-bin-rdw]
