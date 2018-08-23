package com.laboros;

import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSReadService extends Configured implements Tool {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		if(args.length <2) {
			System.out.println("Usage of Read Service");
			return;
		}
		
		Configuration configuration = new Configuration();
		
		try {
			ToolRunner.run(configuration, new HDFSReadService(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		String destination = args[0];
		String fileName = args[1];

		Path path = new Path(destination, fileName);
		
		Configuration configuration = super.getConf();
		
		FileSystem fileSystem = FileSystem.get(configuration);
		
		FSDataInputStream dataInputStream = fileSystem.open(path);
		
		OutputStream outputStream = new FileOutputStream(fileName);
		
		IOUtils.copyBytes(dataInputStream, outputStream, configuration,true);
		
		return 0;
	}

}
