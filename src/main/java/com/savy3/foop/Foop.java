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
		while (i <= args.length) {
			if (args[i].trim() == "--connect"){
				i++;
				job.set("foop.ftp.connection", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--username"){
				i++;
				job.set("foop.ftp.username", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--password"){
				i++;
				job.set("foop.ftp.password", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--password-file"){
				i++;
				job.set("foop.ftp.password.file", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--password-alias"){
				i++;
				job.set("foop.ftp.password.alias", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--source-dir"){
				i++;
				job.set("foop.source", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--target-dir"){
				i++;
				job.set("foop.target", args[i].trim());
				i++;
			}
			if(args[i].trim() == "--ftp-type-ascii"){
				job.set("foop.ftp.type", "ascii");
				i++;
			}
			if(args[i].trim() == "--ftp-type-bin-rdw"){
				job.set("foop.ftp.type", "binaryRDW");
				i++;
			}
			i++;
		}

		System.out.println("FTP to Hadoop!");
		CopyData cd = new CopyData(job);
		cd.run();
	}
}
