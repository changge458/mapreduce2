package com.qst.join.reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<CompKey, Text, Text, NullWritable> {

	/**
	 * 	1	0	1	tom
		1	1	8	no008	6.0	1
		2	0	2	tomas
		2	1	7	no007	5.0	2
		2	1	5	no005	23.1	2
	 */
	@Override
	protected void reduce(CompKey key, Iterable<Text> values, Reducer<CompKey, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		String cinfo = it.next().toString();
		
		while(it.hasNext()){
			String oinfo = it.next().toString();
			String[] arr = oinfo.split("\t");
			String ono = arr[1];
			String price = arr[2];
			String info = cinfo + "\t" + ono + "\t" + price;
			System.out.println(info);
			context.write(new Text(info), NullWritable.get());
		}
		
		
		
		
	}

	
}
