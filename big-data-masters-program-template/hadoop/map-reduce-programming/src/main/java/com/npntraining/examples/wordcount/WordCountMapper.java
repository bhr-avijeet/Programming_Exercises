package com.npntraining.examples.wordcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	//[C,C++,Java] => [0,[C,C++,Java]]
	@Override
	public void map(LongWritable key, Text value, Context output)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split(",");

		for (String split : splits) {
			output.write(new Text(split), new IntWritable(1));
		}
	}
	

}
