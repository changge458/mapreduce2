package com.qst.outputformat.dboutput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, MyDBWritable2, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values ,
			Reducer<Text, IntWritable, MyDBWritable2, NullWritable>.Context context) throws IOException, InterruptedException {

		//hello [1,1]
		Integer num = 0; 
		
		for( IntWritable value : values){
			num += value.get();
		}
		
		MyDBWritable2 mdbw = new MyDBWritable2();
		mdbw.setWord(key.toString());
		mdbw.setCount(num);
		
		context.write(mdbw, NullWritable.get());
	}
	
}
