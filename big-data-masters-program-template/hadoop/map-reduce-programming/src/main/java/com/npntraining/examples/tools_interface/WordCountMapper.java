package com.npntraining.examples.tools_interface;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] splits = line.split(",");

		for (String split : splits) {
			context.write(new Text(split), new IntWritable(1));
		}
	}
}
