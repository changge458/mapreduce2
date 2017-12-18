package com.qst.kmeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	// 中心集合
	ArrayList<ArrayList<Double>> centers = null;
	// 用k个中心
	int k = 0;

	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// 读取中心
		try {
			centers = Utils.getCentersFromHDFS(context.getConfiguration().get("cPath"), false);
			//中心文件的行数
			k = centers.size();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 1.每次读取一条要分类的条记录与中心做对比，归类到对应的中心 2.以中心ID为key，中心包含的记录为value输出(例如： 1 0.2 。
	 * 1为聚类中心的ID，0.2为靠近聚类中心的某个值)
	 */

	// 读取一行数据
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {

		ArrayList<Double> fileds = Utils.textToArray(value);
		int sizeOfFileds = fileds.size();

		double minDistance = Double.MAX_VALUE;
		int centerIndex = 0;

		// 依次取出k个中心点与当前读取的记录做计算
		for (int i = 0; i < k; i++) {
			double currentDistance = 0;
			for (int j = 0; j < sizeOfFileds; j++) {
				double centerPoint = centers.get(i).get(j);
				double filed = fileds.get(j);
				currentDistance += Math.pow( centerPoint - filed , 2);
			}
			// 循环找出距离该记录最接近的中心点的ID
			if (currentDistance < minDistance) {
				minDistance = currentDistance;
				centerIndex = i;
			}
		}
		// 以中心点为Key 将记录原样输出
		context.write(new IntWritable(centerIndex + 1), value); 
	}

}
