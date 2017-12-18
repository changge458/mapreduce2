package com.qst.join.reduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance();
		
		job.setJobName("reduce Join");
		
		job.setJarByClass(App.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path("D:/wc"));
		FileOutputFormat.setOutputPath(job, new Path("D:/wc_out"));
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("D:/wc_out"))){
			fs.delete(new Path("D:/wc_out"), true);
		}
		
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setPartitionerClass(Partition.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
		job.setOutputKeyClass(CompKey.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : -1);
		
	}

}
