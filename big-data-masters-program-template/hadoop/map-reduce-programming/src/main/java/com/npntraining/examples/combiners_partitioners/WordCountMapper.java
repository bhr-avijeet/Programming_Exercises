package com.npntraining.examples.combiners_partitioners;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	// hadoop supported data types
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// LongWritable : byte offset
	// Text value : entire line
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String names[] = line.split(" ");
		for (String str : names) {
			word.set(str);
			output.collect(word, one);
		}
	}
}
