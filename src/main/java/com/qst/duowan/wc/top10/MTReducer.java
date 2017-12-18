package com.qst.duowan.wc.top10;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MTReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	TreeMap<Integer, String> tm = new TreeMap<Integer, String>();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		// context.write(new Text("========================"), null);
		for(IntWritable value : values){
			tm.put(value.get(), key.toString());
			if (tm.size() > 10) {
				tm.remove(tm.firstKey());
			}
		}
	}
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		NavigableSet<Integer> keyset = tm.descendingKeySet();
		for(Integer key : keyset){
			context.write(new Text(tm.get(key)) , new IntWritable(key));
		}
	}

}
