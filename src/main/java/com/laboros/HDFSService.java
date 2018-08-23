package com.laboros;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		
		//java HDFSService w.txt /user/trianfd
		
		
		//Writing data set  Metadata  + Data
		String destinationDir = args[1];
		String fileName = args[0];
		
		Path destinationDirWithFile =new Path(destinationDir, fileName);
		
		FileSystem fs = FileSystem.get(super.getConf());
		//Edge node RAM 64GB
		
		//Edge node local 
		//Fs.defaultFS  conf for identify the whichfile system using 
		//fs.defaultFS = file// local file system
		//fs.defaultFS = hdfs// HDFSl file system
		//fs.default 
		//HDFS always thinks file as a URI
		FSDataOutputStream dataOutputStream = fs.create(destinationDirWithFile); //Edge Node
		//Name node will check for validate file name already exist 
		//Name checks permissions available or not
		
		// FS pool for serving request
		//Never create any object Name Noe
		
		//Writing data
		// 1)Split Data 
			//a. Name node will split data in to Blocks
			 // each block size 128MB  1TB bata blocks 8192
			//Blocks split happend at Edge node with help of FSDataOUt
		
		//2) Identify  Data Block have to data Node
			//Block Placement policy 
			//Block Management service will handle the BPP
			//BMS is part of NN
		
		//3) Writing
			//Write Data Block will be written by Edge node
			//Data node will allow Client to 
		
		//4) Meeting Replica 
			//Only First Block will writen by NN
			//Data Node will process the repilica 
			
		//5) Sync Meta Data
			//Data node 
			
		
		//InputS
		
		InputStream inputStream = new FileInputStream(fileName);
		
		IOUtils.copyBytes(inputStream, dataOutputStream, super.getConf(), true);
		
		//
		return 0;
	}

}
