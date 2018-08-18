package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) {
		
		if(args.length<2){
			System.out.println("Usage HDFS Service : EDGE/PATH input HDFS /Destination/path");
			return;
		}
		
		//Loading configuration 
		Configuration configuration = new Configuration();
		
		try {
			ToolRunner.run(configuration, new HDFSService(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Writing data set  Metadata  + Data
		String destinationDir = args[1];
		String fileName = args[0];
		
		Path destinationDirWithFile =new Path(destinationDir, fileName);
		
		FileSystem fs = FileSystem.get(super.getConf());
		
		//Edge node local 
		//Fs.defaultFS  conf for identify the whichfile system using 
		//fs.defaultFS = file// local file system
		//fs.defaultFS = hdfs// HDFSl file system
		//HDFS always thinks file as a URI
		fs.create(destinationDirWithFile);
		//Name node will check for validate file name already exist 
		//Name checks permissions available or not
		
		
		//
		return 0;
	}

}
