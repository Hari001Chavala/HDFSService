package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

	public static void main(String[] args) {
		
		if(args.length<2) {
			System.out.println("Usage Driver Class : "+WordCountDriver.class.getName()+" input HDFS /Destination/path output HDFS /Destination/path ");
			return;
		}
		
		Configuration configuration = new Configuration();
		
		try {
			ToolRunner.run(configuration, new WordCountDriver(), args);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public int run(String[] args) throws Exception {
		
		Configuration configuration = super.getConf();
		
		Job wordCountJob = Job.getInstance(configuration, WordCountDriver.class.getName());
		
		wordCountJob.setJarByClass(WordCountDriver.class);
		//Is setting the class path of Mapper class with the help of driver class on Mapper data node 
		
		
		// 4. Set Input
			String input =args[0];
			Path hdfsInput = new Path(input);
			TextInputFormat.addInputPath(wordCountJob, hdfsInput);
			
		// 5. SetOutput
			String output =args[1];
			Path hdfsOutput = new Path(output);
			TextOutputFormat.setOutputPath(wordCountJob, hdfsOutput);
		// 6. SetMapper
			wordCountJob.setMapperClass(WordCoundMapper.class);
		// 7. SetReducer
			wordCountJob.setReducerClass(WordCountReducer.class);
		// 8. Set MapOutPut - 
		
		
		
		
		
		
		
		
		
		
		
		
		
		return 0;
	}

}
