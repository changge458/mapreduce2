package com.qst.join.map;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance();
		
		job.setJobName("Map Join");
		
		job.setJarByClass(App.class);
		
		//分布式缓存
		job.addCacheFile(new URI("file:///D:/wc/customers.txt"));
		
		FileInputFormat.addInputPath(job, new Path("/join/orders.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/join_out"));
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("/join_out"))){
			fs.delete(new Path("/join_out"), true);
		}
		
		job.setMapperClass(JoinMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : -1);
		
		
	}

}
