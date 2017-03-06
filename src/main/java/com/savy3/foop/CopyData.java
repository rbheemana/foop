package com.savy3.foop;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;

public class CopyData {
	FTPFileSystem ftpfs;
	Configuration conf;
	FTPHelper ftph;
	CopyData() throws Exception{
		conf = new Configuration();
		ftpfs = new FTPFileSystem();
		ftpfs.setConf(conf);
		ftph = new FTPHelper(conf);
	}
	CopyData(Configuration jConf) throws Exception{
		conf = jConf;
		ftph = new FTPHelper(conf);
		ftpfs = ftph.getFTPFileSystem();
		ftpfs.setConf(conf);
	}
	
	void run() throws IOException, URISyntaxException{
		ftpfs.initialize(ftph.getHost(), conf);
		FSDataInputStream fsdin = ftpfs.open(ftph.getSource(), 1000);
		FileSystem fileSystem=FileSystem.get(conf);
		OutputStream outputStream=fileSystem.create(ftph.getTarget());
		java.util.Date date= new java.util.Date();
		System.out.println((new Timestamp(date.getTime()))+",\t FTP for file "+ftph.getSource()+" started." );
		IOUtils.copyBytes(fsdin, outputStream, conf, true);
		System.out.println((new Timestamp(date.getTime()))+",\t FTP for file "+ftph.getSource()+" Finished." );
	}
	
}
