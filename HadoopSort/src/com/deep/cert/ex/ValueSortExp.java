package com.deep.cert.ex;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ValueSortExp {

	// public static void main(String[] args) {
	// TODO Auto-generated method stub
	void sort(String[] args) {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = null;
		try {
			job = new Job(conf, "test");
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		job.setJarByClass(ValueSortExp.class);

		// Setup MapReduce
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(IntComparator.class);
		// Input
		try {
			FileInputFormat.addInputPath(job, inputPath);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * // Delete output if exists FileSystem hdfs = FileSystem.get(conf); if
		 * (hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
		 * 
		 * // Execute job int code = job.waitForCompletion(true) ? 0 : 1;
		 * System.exit(code);
		 */

		System.out.println("### here");

		FileSystem hdfs;
		try {
			hdfs = FileSystem.get(conf);

			if (hdfs.exists(outputDir))
				hdfs.delete(outputDir, true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// Execute job
		int code = 0;
		try {
			code = job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.exit(code);
	}

	public static class IntComparator extends WritableComparator {

		public IntComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

			return v1.compareTo(v2) * (-1);
		}
	}

	// public static class MapTask extends
	// Mapper<LongWritable, Text, IntWritable, IntWritable> {
	// public void map(LongWritable key, Text value, Context context)
	// throws java.io.IOException, InterruptedException {
	// String line = value.toString();
	// String[] tokens = line.split(","); // This is the delimiter between
	// int keypart = Integer.parseInt(tokens[0]);
	// int valuePart = Integer.parseInt(tokens[1]);
	// context.write(new IntWritable(valuePart), new IntWritable(keypart));
	//
	// }
	// }

	// class ReduceTask extends
	// Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	// public void reduce(IntWritable key, Iterable<IntWritable> list,
	// Context context) throws java.io.IOException,
	// InterruptedException {
	//
	// for (IntWritable value : list) {
	//
	// context.write(value, key);
	//
	// }
	//
	// }
	// }

}
