package com.savy3.foop;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.conf.Configuration;


//import com.savy3.foop.FoopMR.FTPMapper;

public class Foop {
	public static void main(String[] args) throws Exception {
		//Configuration conf = new JobConf(Foop.class);
		Configuration conf = new Configuration();
		
		
		int i = 0;
		while (i < args.length) {
			if (args[i].trim().equalsIgnoreCase("--connect")){
				i++;
				System.out.println(args[i].trim());
				conf.set("foop.ftp.connection", args[i].trim());
				i++;
			}
			else if(args[i].trim().equalsIgnoreCase("--username")){
				i++;
				conf.set("foop.ftp.username", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password")){
				i++;
				conf.set("foop.ftp.password", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password-file")){
				i++;
				conf.set("foop.ftp.password.file", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password-alias")){
				i++;
				conf.set("foop.ftp.password.alias", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--source-dir")){
				i++;
				conf.set("foop.source", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--target-dir")){
				i++;
				conf.set("foop.target", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--ftp-type-ascii")){
				conf.set("foop.ftp.type", "ascii");
				conf.setInt(FTPFileSystem.FS_FTP_TRANSFER_MODE, FTP.ASCII_FILE_TYPE);
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--ftp-type-bin-rdw")){
				conf.setInt(FTPFileSystem.FS_FTP_TRANSFER_MODE, FTP.BINARY_FILE_TYPE);
				conf.setBoolean(FTPFileSystem.FS_FTP_GET_RDW, true);
				i++;
			}else{
				i++;
			}
		}
		conf.setBoolean(FTPFileSystem.FS_FTP_LOCAL_PASSIVE, true);
		System.out.println("FTP to Hadoop!");
		CopyData cd = new CopyData(conf);
		cd.run();

	}
}
