package com.qst.kmeans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KApp {

	public static void run(String centerPath, String dataPath, String newCenterPath, boolean runReduce)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("cPath", centerPath);

		Job job = Job.getInstance(conf);
		job.setJarByClass(KApp.class);

		job.setMapperClass(KMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		if (runReduce) {
			// 最后依次输出不许要reduce
			job.setReducerClass(KReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		}

		FileInputFormat.addInputPath(job, new Path(dataPath));

		FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

		System.out.println(job.waitForCompletion(true));
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		String centerPath = "hdfs://localhost:8020/input/center.txt";
		String dataPath = "hdfs://localhost:8020/input/wine.txt";
		String newCenterPath = "hdfs://localhost:8020/kmean";

		int count = 0;

		while (true) {
			run(centerPath, dataPath, newCenterPath, true);
			System.out.println(" 第 " + ++count + " 次计算 ");
			//如果distance == 0 ; 则无需reduce
			if (Utils.compareCenters(centerPath, newCenterPath)) {
				run(centerPath, dataPath, newCenterPath, false);
				break;
			}
		}
	}

}
