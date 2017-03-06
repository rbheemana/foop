package com.savy3.foop;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

public class Foop {
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		Configuration job = new JobConf(Foop.class);
		int i = 0;
		while (i < args.length) {
			if (args[i].trim().equalsIgnoreCase("--connect")){
				i++;
				System.out.println(args[i].trim());
				job.set("foop.ftp.connection", args[i].trim());
				i++;
			}
			else if(args[i].trim().equalsIgnoreCase("--username")){
				i++;
				job.set("foop.ftp.username", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password")){
				i++;
				job.set("foop.ftp.password", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password-file")){
				i++;
				job.set("foop.ftp.password.file", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--password-alias")){
				i++;
				job.set("foop.ftp.password.alias", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--source-dir")){
				i++;
				job.set("foop.source", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--target-dir")){
				i++;
				job.set("foop.target", args[i].trim());
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--ftp-type-ascii")){
				job.set("foop.ftp.type", "ascii");
				i++;
			}else if(args[i].trim().equalsIgnoreCase("--ftp-type-bin-rdw")){
				job.set("foop.ftp.type", "binaryRDW");
				i++;
			}else{
				i++;
			}
		}
		System.out.println("FTP to Hadoop!");
		CopyData cd = new CopyData(job);
		cd.run();
	}
}
