package com.deep.cert.ex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

class ReduceTask extends
		Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	public void reduce(IntWritable key, Iterable<IntWritable> list,
			Context context) throws java.io.IOException, InterruptedException {

		for (IntWritable value : list) {

			context.write(value, key);

		}

	}
}
