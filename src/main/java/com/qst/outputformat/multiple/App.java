package com.qst.outputformat.multiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {

	public static void main(String[] args) throws Exception {

		//System.setProperty("HADOOP_USER_NAME", "centos");

		Job job = Job.getInstance();

		// mapreduce.job.jar
		job.setJarByClass(App.class);

		// mapreduce.job.name
		job.setJobName("word count");
		

		// 设置输入输出路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//注册输出类型，设置输出格式
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
		
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		

		//job.setNumReduceTasks(5);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置输出kv
		// mapreduce.job.output.key.class
		// mapreduce.job.output.value.class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		

		// 指定map和reduce的类
		// mapreduce.job.map.class
		// mapreduce.job.reduce.class
		job.setMapperClass(WCMapper.class);
		
		
		//job.setCombinerClass(WCCombiner.class);
		job.setReducerClass(WCReducer.class);

		// Iterator<Entry<String, String>> it = conf.iterator();
		// while(it.hasNext()){
		// Entry<String, String> entry = it.next();
		// System.out.println(entry.getKey() + " = " + entry.getValue());
		// }
		
		//FileInputFormat.setMaxInputSplitSize(job, 200);
		//FileInputFormat.setMinInputSplitSize(job, 500);
		System.exit((job.waitForCompletion(true) ? 0 : 1));
	}

}
