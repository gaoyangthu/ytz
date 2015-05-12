package com.github.gaoyangthu.ytz.mapreduce;

import com.github.gaoyangthu.ytz.security.tripledes.Encryptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/11/26 0026
 */
public class OdsNbrEncrypt {
	public static class MapClass extends Mapper<Object, Text, Text, Text> {
		/* Encryptor key */
		private static final String EN_KEY = "asiainfo3Des";

		/**
		 * Map phase
		 *
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\t", -1);
			Text k = new Text(items[0]);
			//Text k = new Text(Encryptor.getEncryptedString(EN_KEY, items[0]));
			StringBuilder sb = new StringBuilder("");
			sb.append(items[1]).append("\t");
			sb.append(items[2]).append("\t");
			sb.append(items[3]).append("\t");
			sb.append(items[4]);
			Text v = new Text(sb.toString());
			context.write(k, v);
		}
	}

	public static  class ReduceClass extends Reducer<Text, Text, Text, Text> {
		/* Encryptor key */
		private static final String EN_KEY = "asiainfo3Des";

		/**
		 * Reduce phase
		 *
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public  void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value: values) {
				Text k = new Text(Encryptor.getEncryptedString(EN_KEY, key.toString()));
				context.write(k, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * Set a job and its configurations
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: OdsNbrEncrypt <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "ods accs_nbr encrypt");
		job.setJarByClass(OdsNbrEncrypt.class);

		/**
		 * set the class of mapper and reducer
		 */
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		//job.setNumReduceTasks(0);

		/**
		 * Set output class of mapper and reducer
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		/**
		 * Set input and output files path of the job
		 */
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
