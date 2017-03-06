package com.savy3.foop;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class FTPHelper {
	String host, source, target, ftpType;
	public static final String FTP_CONN = "foop.ftp.connection";
	public static final String FTP_USER = "foop.ftp.username";
	public static final String FTP_PASS = "foop.ftp.password";
	public static final String SOURCE_LOC = "foop.source";
	public static final String TARGET_LOC = "foop.target";
	public static final String FTP_TYPE = "foop.ftp.type";
	FTPHelper(Configuration conf){
		setHost(conf.get(FTP_CONN),conf.get(FTP_USER),conf.get(FTP_PASS));
		setSource(conf.get(SOURCE_LOC));
		setTarget(conf.get(TARGET_LOC));
		setFtpType(conf.get(FTP_TYPE));
		
	}
	public void setFtpType(String ftpType) {
		if (ftpType == null){
			this.ftpType = "binary";
		}else{
			this.ftpType = ftpType;
		}
	}
	public String getFtpType() {
		return this.ftpType;
	}
	public void setHost(String host, String user, String password) {
		System.out.println(host+user+password);
		this.host = "ftp://"+user.trim()+":"+password.trim()+"@"+host;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public URI getHost() throws URISyntaxException {
			return new URI(host);
	}

	public Path getSource() {
		return new Path(source);
	}

	public Path getTarget() {
		return new Path(target);
	}
	public FTPFileSystem getFTPFileSystem() {
		if (getFtpType() == "binaryRDW"){
			return new FTPBinaryRDWFileSystem();
		}else if (getFtpType() == "ascii"){
			return new FTPASCIIFileSystem();
		}
		return new FTPFileSystem();
	}

}
