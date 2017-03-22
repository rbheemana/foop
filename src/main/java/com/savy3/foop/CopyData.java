package com.savy3.foop;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
		ftpfs = ftph.getFTPFileSystem(conf);
		ftpfs.setConf(conf);
	}
	boolean isDirectory() throws IOException, URISyntaxException{
		ftpfs.initialize(ftph.getHost(), conf);
		URI uri = ftph.getHost();
		return !ftpfs.isFile(ftph.getSource());
	}
	
	void run() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		ftpfs.initialize(ftph.getHost(), conf);
		URI uri = ftph.getHost();
		System.out.println(uri.getHost() + ":" + uri.getUserInfo());
		System.out.println("Input file:"+ftph.getSource());
		FileSystem fileSystem = FileSystem.get(conf);
		
		
		if (ftpfs.isFile(ftph.getSource())) {
			System.out.println("Working on file "+ftph.getSource());
			FSDataInputStream fsdin = ftpfs.open(ftph.getSource(), 1000);
			Path f = ftph.getOutputLocation();
			System.out.println("Output file Path:"+f);
			OutputStream outputStream = fileSystem.create(f);
			java.util.Date date = new java.util.Date();
			System.out.println((new Timestamp(date.getTime()))
					+ ",\t FTP for file " + ftph.getSource() + " started.");
			IOUtils.copyBytes(fsdin, outputStream, conf, true);
			System.out.println((new Timestamp(date.getTime()))
					+ ",\t FTP for file " + ftph.getSource() + " Finished.");
		}else{
			System.out.println("Directory is specified, Preparing Map-Reduce Job..");
			
		    FSDataOutputStream outputStream;
		    int i=0;
			for (Path p: ftpfs.listFiles(ftph.getSource())){
				outputStream = fileSystem.create(new Path(ftph.getMRInputFolder(),""+i));
				String filename = ""+p.getParent().toString()+"/"+p.getName().toString();
				outputStream.writeBytes(filename.trim()+"\n");
				outputStream.close();
				i++;
			}
			
			System.out.println("Input to Map-Reduce:"+ftph.getMRInputFolder());
			Job jobMR = Job.getInstance(conf, "FTP to Hadoop");
			jobMR.setJarByClass(Foop.class);
			jobMR.setMapperClass(FoopMR.class);
			jobMR.setOutputKeyClass(Text.class);
			jobMR.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(jobMR, ftph.getMRInputFolder());
			FileOutputFormat.setOutputPath(jobMR,ftph.getMROutputputFolder());
			if(jobMR.waitForCompletion(true)){
				fileSystem.delete(ftph.getStagingFolder());
				System.exit(0);
			}else{
				System.exit(1);
			}
		}
	}
	
}
