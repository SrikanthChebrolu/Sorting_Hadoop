package com.deep.cert.ex;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.deep.cert.ex.*;


public final class RandomInteger {
	static FSDataOutputStream fin;
	static Configuration config = new Configuration();

	public static void main(String args[]) throws IOException {
		// Path filenamePath = new Path("/user/input/test/test.txt");
		Path filenamePath = new Path(args[0]);
		FileSystem fs = FileSystem.get(config);
		System.out.println(fs.exists(filenamePath));
		ValueSortExp val = new ValueSortExp();
		try {
			if (fs.exists(filenamePath)) {
				fs.delete(filenamePath, true);
			}
			// outputStream = new FileOutputStream("E:/demo/demo.txt");
			// outputStreamWriter = new OutputStreamWriter(outputStream,
			// "UTF-16");
			// bufferedWriter = new BufferedWriter(outputStreamWriter);
			fin = fs.create(filenamePath);

			Random randomGenerator = new Random();
			int count = 0;
			for (int idx = 1; idx <= 1000; idx++) {
				System.out.println(idx);
				int randomInt = count++;
				int randomno = randomGenerator.nextInt(2000);
				try {
					log((new StringBuilder(String.valueOf(randomInt)))
							.append(",").append(randomno).append("\n")
							.toString());
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			System.out.println("DATA WRITE COMPLETED.");
			fin.close();
			long start = System.currentTimeMillis();
			System.out.println("SORTING PROCESS STARTED at : " + start);
			val.sort(args);
			System.out.println("SORTING PROCESS ENDED at : "
					+ (System.currentTimeMillis() - start) / 1000);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void log(String aMessage) throws IOException {
		// System.out.println(aMessage);
		// bufferedWriter.write(aMessage);
		fin.writeBytes(aMessage);
	}

	static FileOutputStream outputStream;
	static OutputStreamWriter outputStreamWriter;
	static BufferedWriter bufferedWriter;
}