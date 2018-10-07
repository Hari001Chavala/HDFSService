package com.laboros.mapper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, Text>{

	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String inputLine = value.toString();
		long InputKey = key.get();
		System.out.println("KEY: "+InputKey+"  "+"VALUE: "+inputLine);
		
		if(StringUtils.isNotBlank(inputLine)) {
			
			String date = StringUtils.substring(inputLine, 6, 14);
			
			String year = StringUtils.substring(date, 0, 4);
			
			String maxTemp = StringUtils.substring(inputLine, 38, 45);
			
			context.write(new Text(year), new Text(date+"\t"+maxTemp));
			
			
		}
				
	}
	
	
}
