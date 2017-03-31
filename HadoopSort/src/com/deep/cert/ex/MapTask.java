package com.deep.cert.ex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapTask extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {
	public void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(","); // This is the delimiter between
		int keypart = Integer.parseInt(tokens[0]);
		int valuePart = Integer.parseInt(tokens[1]);
		context.write(new IntWritable(valuePart), new IntWritable(keypart));

	}
}
