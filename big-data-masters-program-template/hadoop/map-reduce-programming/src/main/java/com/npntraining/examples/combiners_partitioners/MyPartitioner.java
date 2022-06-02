package com.npntraining.examples.combiners_partitioners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner implements Partitioner<Text, IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		String lowerKey = key.toString().toLowerCase();

		if (key.toString().equalsIgnoreCase("Java") || lowerKey.equalsIgnoreCase("J2EE")) {
			return 0;
		} else if (key.toString().startsWith("P")) {
			return 1;
		} else {
			return 2;
		}
	}

}
