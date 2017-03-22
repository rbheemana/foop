package com.savy3.foop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


  public class FoopMR
       extends Mapper<Object, Text, Text, IntWritable>{



    private Configuration conf;
    com.savy3.foop.FTPFileSystem ftpfs;
    com.savy3.foop.FTPHelper ftph;


    @Override
    public void setup(Context context) throws IOException,
        InterruptedException {
      conf = context.getConfiguration();
      try {
		ftph = new FTPHelper(conf);
      } catch (Exception e) {
		// TODO Auto-generated catch block
    	  ftph = null;
		e.printStackTrace();
      }
    }



    @Override
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException  {
    	
    	String fileToFTP = value.toString().trim();
    	if(fileToFTP.isEmpty()){
    		return;
    	}
    	ftph.setSourceFile(fileToFTP);
    	System.out.println("Input File name:"+fileToFTP);
		//conf.set(FTPHelper.SOURCE_LOC, fileToFTP);
    	ftpfs = ftph.getFTPFileSystem(conf);
		ftpfs.setConf(conf);
		URI uri = null;
		try {
			ftpfs.initialize(ftph.getHost(), conf);
			uri = ftph.getHost();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(uri.getHost() + ":" + uri.getUserInfo());
		
		if (!ftpfs.isDirectory(ftph.getSourceFile())) {
			FileSystem fileSystem = FileSystem.get(conf);
			System.out.println("Working on file "+ftph.getSourceFile());
			FSDataInputStream fsdin = ftpfs.open(ftph.getSourceFile(), 1000);
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
			throw new InterruptedException("Directory recieved as Input-"+ftph.getSourceFile());
		}
    }
  }

