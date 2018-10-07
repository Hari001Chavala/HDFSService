package com.laboros.reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends Reducer<Text, Text, Text, Text>{

	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
        
		String computedMaxTempDate = null;
		float maxTempOnDate = Float.MIN_VALUE;
		
		
		for(Text value : values) {
			String dateTemparature = value.toString();
			String tokens[] = StringUtils.splitPreserveAllTokens(dateTemparature, "\t");
			String tempDate = tokens[0];
			float tempMapTemp = Float.parseFloat(tokens[1]);
			if(maxTempOnDate<tempMapTemp) {
				maxTempOnDate = tempMapTemp;
				computedMaxTempDate = tempDate;
			}
		}
		
		context.write(key, new Text(computedMaxTempDate+"/t"+maxTempOnDate));
		
		
		
	}
	
	
	
}
