package com.qst.sort.total;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class App {

	@SuppressWarnings({ "unchecked", "rawtypes" })
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

		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		job.setNumReduceTasks(6);

		// 设置输出kv
		// mapreduce.job.output.key.class
		// mapreduce.job.output.value.class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 指定map和reduce的类
		// mapreduce.job.map.class
		// mapreduce.job.reduce.class
		job.setMapperClass(WCMapper.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		TotalOrderPartitioner.setPartitionFile(conf, new Path("D:/seq/out/par.seq"));
		
		/**
		 * freq 一个key被选中的几率
		 * numSamples 样本总数.
		 * maxSplitsSampled 一个切片检查的最大数量. 
		 */
		//InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler(0.1, 500,10);
		//InputSampler.Sampler<Text, Text> sampler = new InputSampler.IntervalSampler(0.1, 500);
		InputSampler.Sampler<Text, Text> sampler = new InputSampler.SplitSampler(500,10);
		InputSampler.writePartitionFile(job, sampler);
		
		
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
