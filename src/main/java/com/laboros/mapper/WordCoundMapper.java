package com.laboros.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoundMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		//An InputFormat for plain text files. Files are broken into lines. 
		//Either linefeed or carriage-return are used to signal end of line.
		//Keys are the position in the file, and values are the line of text..
		//key 0;    LongWritable
		//value Line of the file (Text) 
		
		String InputLine = value.toString();
		long InputKey = key.get();
		System.out.println("KEY: "+InputKey+"  "+"VALUE: "+InputLine);
		
		if(StringUtils.isNotBlank(InputLine)) {
			String words[] = StringUtils.splitPreserveAllTokens(InputLine," ");
			
			for (String word :words) {
				context.write(new Text(word), new IntWritable(1));
			}
			
		}
				
		
		
		
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
	
}
