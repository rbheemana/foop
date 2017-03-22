# foop
This Project is built to FTP the files from the FTPServer to HDFS parallely. 

Purpose of this project as below:
1. Pull data from any FTP server. Ex: Mainframe, FTP, etc
2. Multiple files from FTP server needs to be downloaded in parallel 
3. Files should directly pushed to HDFS

Command Syntax:
hadoop jar <jar_local_location> com.savy3.foop.Foop --connect {ftp_url} --username {userid} --password {password} --source-dir {source file/dir} --target-dir {target hdfs dir} [--ftp-type-ascii|--ftp-type-bin-rdw]

Ex:
hadoop jar /jar/foop.jar com.savy3.foop.Foop --connect gnu.org  --source-dir /gnu/GNUinfo/  --target-dir /data/gnu --username anonymous --password null

Output:
 /data/gnu/Audio
 /data/gnu/Audio/README
 /data/gnu/Audio/francais
 /data/gnu/Audio/francais/index.html
 /data/gnu/Audio/francais/ogg_readme.txt
 /data/gnu/Audio/francais/rms-french-interview-2001-11-20.tar
 /data/gnu/Audio/francais/rms-interview-paris-27-jan-2002.ogg
 /data/gnu/Audio/francais/rms-speech-paris-30-jan-2002.ogg
 /data/gnu/Audio/index.html
 /data/gnu/Audio/index.txt
 /data/gnu/Audio/rms-speech-arsdigita2001.ogg
 /data/gnu/Audio/rms-speech-cambridgeuni-england2002.ogg
 /data/gnu/Audio/rms-speech-cglug2000.ogg
 /data/gnu/Audio/rms-speech-linuxtag2000.ogg
 /data/gnu/Audio/rms-speech-mit2001.ogg
 /data/gnu/Audio/rms-speech-nyu2001.ogg
 /data/gnu/Audio/rms-speech-qmul-london2002.ogg
 /data/gnu/README
