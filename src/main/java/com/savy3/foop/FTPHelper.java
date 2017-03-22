package com.savy3.foop;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class FTPHelper {
	String host, source, target, ftpType;
	private String sourceFile;
	public static final String FTP_CONN = "foop.ftp.connection";
	public static final String FTP_USER = "foop.ftp.username";
	public static final String FTP_PASS = "foop.ftp.password";
	public static final String SOURCE_LOC = "foop.source";
	public static final String SOURCE_FILE = "foop.source.file";
	public static final String TARGET_LOC = "foop.target";
	public static final String FTP_TYPE = "foop.ftp.type";
	

	FTPHelper(Configuration conf) throws Exception{
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

	public void setHost(String host, String user, String password) throws Exception {
		
		if(user == null){
			this.host = "ftp://ftp."+host+"/";
		}else if(password != null){
			this.host = "ftp://"+user.trim()+":"+password.trim()+"@ftp."+host+"/";
		} else if(password == null) {
			this.host = "ftp://"+user.trim()+":@ftp."+host;
		}
		System.out.println("Connecting to:"+this.host);
	}

	public void setSource(String source) {
		this.source = source;
	}
	public void setSourceFile(Configuration conf) {
		this.sourceFile = conf.get(SOURCE_FILE,null);
	}
	public void setSourceFile(String sourceFile) {
		this.sourceFile = sourceFile;
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
	public Path getSourceFile() {
		return new Path(sourceFile);
	}

	public Path getTarget() {
		return new Path(target);
	}
	public FTPFileSystem getFTPFileSystem(Configuration conf) {
		if (getFtpType() == "binaryRDW"){
			conf.setInt(FTPFileSystem.FS_FTP_TRANSFER_MODE, FTP.BINARY_FILE_TYPE);
			conf.setBoolean(FTPFileSystem.FS_FTP_GET_RDW, true);
		}else if (getFtpType() == "ascii"){
			conf.setInt(FTPFileSystem.FS_FTP_TRANSFER_MODE, FTP.ASCII_FILE_TYPE);
		}
		return new FTPFileSystem();
	}
	public Path getOutputLocation(Path spath) throws IllegalArgumentException {
		return getOutputLocation(spath,this.target);
	}
	public Path getOutputLocation(Path spath, String ofolder) throws IllegalArgumentException {
		String sfolder = this.source;
		if (!sfolder.endsWith("/")) {
				sfolder += "/";
		}
		if (!sfolder.startsWith("/")) {
			sfolder += "/";
		}
		String relative = spath.toString().substring(spath.toString().indexOf(sfolder)+sfolder.length());
		if (!ofolder.endsWith("/")) {
				ofolder += "/";
		}
		if(relative.isEmpty()){
			System.out.println("folder"+ofolder+";relative:"+relative);
			throw new IllegalArgumentException("Relative path is Empty; Source folder :"+ this.source+"File Path :"+spath.toString());
		}
		
		return new Path(ofolder,relative);
	}
	public Path getOutputLocation() throws IllegalArgumentException {
		return getOutputLocation(new Path(this.sourceFile));
	}
	public Path getStagingFolder(){
		String ts = "2017-03";
		String stagingFolder = this.target+"/.foop_input_"+ts;
		if (!stagingFolder.endsWith("/")) {
			stagingFolder += "/";
		}
		if (!stagingFolder.startsWith("/")) {
			stagingFolder += "/";
		}
		return new Path(stagingFolder);
	}
	public Path getMRInputFolder(){
		
		return new Path(getStagingFolder(),"input");
	}
	public Path getMROutputputFolder(){
		
		return new Path(getStagingFolder(),"output");
	}

}
