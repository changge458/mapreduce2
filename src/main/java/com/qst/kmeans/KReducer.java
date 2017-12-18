package com.qst.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<IntWritable, Text, Text, Text> {

	/**
	 * 1.Key为聚类中心的ID(索引) value为该中心的记录集合 
	 * 2.计数所有记录元素的平均值，求出新的中心
	 */
	protected void reduce(IntWritable key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();

		//依次读取记录集，每行为一个ArrayList<Double>
		//将相同key值的所有元素进行迭代，并将其字段添加到临时list中
        for(Iterator<Text> it =value.iterator();it.hasNext();){
            ArrayList<Double> tempList = Utils.textToArray(it.next());
            filedsList.add(tempList);
        }
        
        //产生新的中心点
        int filedSize = filedsList.get(0).size();
        double[] avg = new double[filedSize];
        //fieldsize：每行的元素个数
        for(int i=0;i<filedSize;i++){
            //求每列的平均值
            double sum = 0;
            //filedsList为最大的嵌套集合
            int size = filedsList.size();
            for(int j=0;j<size;j++){
                sum += filedsList.get(j).get(i);
            }
            avg[i] = sum / size;
        }
        context.write(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
    }

}
