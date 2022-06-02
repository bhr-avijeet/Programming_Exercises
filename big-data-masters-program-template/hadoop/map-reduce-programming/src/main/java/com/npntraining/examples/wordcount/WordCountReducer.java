package com.npntraining.examples.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	// [C,[1,1,1]
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable val : values) {
			total = total + val.get();
		}
		context.write(key, new IntWritable(total));
	}
}
