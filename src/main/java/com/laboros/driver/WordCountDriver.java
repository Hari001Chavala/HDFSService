package com.laboros.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WordCoundMapper;
import com.laboros.reducer.WordCountReducer;

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
		String input =args[0];
		Path hdfsInput = new Path(input);
		TextInputFormat.addInputPath(wordCountJob, hdfsInput);
		
		String output =args[1];
		Path hdfsOutput = new Path(output);
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOutput);
		
		wordCountJob.setMapperClass(WordCoundMapper.class);
		
		wordCountJob.setReducerClass(WordCountReducer.class);
		
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(LongWritable.class);
		
		//Is setting the class path of Mapper class with the help of driver class on Mapper data node 
		
		
		// 4. Set Input
			
			
		// 5. SetOutput
			
		// 6. SetMapper
			
		// 7. SetReducer
			
		// 8. Set MapOutPut -
			//wordCountJob.setInputFormatClass(cls);
			
		
		
		
		// When Mapper ommits data to OutputCollector
		//Output collector in Context
		//Output collector will have Circular Memory who's size is 256MB
		//In this Memory data will be sorted and when data reaches 256MB or mapper execution completed, the data will be send to Spilled File(which is Sequential File) 
		// Each Sequential file contains compartments. Total number of compartments will be based on total Reducers in the application.
		
		
		/*
		 ------------------------------------------------------------
		 
		 1) IputFormat for the Mapper program will be decided by input data provided by Business people (Business Test Case)
		 2) Depends on Input data developer will decide InputFormat
		 3) Mapper Program will ommit the data to OutputController
		 4) OutputControl will have circular memory with capacity 256MB
		 5) Merg sort will be performed on top of Data ommitted by mapper.
		 6) When memory is full (256 MB reached ) data will be written to Spilled File local to Data Node
		 7) or When Mapper program is completed.
		 8) Spilled files contains compartments depends on number of reducers.
		 9) Partinear will decide which key goes to with compartment, using hasing technich using formula : hash(Key)%Number of Reducers
		 10) Combainer is a mini reducer works at mapper side. 
		
		*/
			
		
		
		
		
		
		
		
		return wordCountJob.waitForCompletion(true) ? 0 : 1;
	}

}
