package com.qst.join.map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	Map<String, String> map = new HashMap<String, String>();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		URI uri = context.getCacheFiles()[0];
		InputStream is = uri.toURL().openStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while((line = br.readLine()) != null){
			String[] arr = line.split("\t");
			String id = arr[0];
			map.put(id, line);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split("\t");
		String orderno = arr[1]; 
		String price = arr[2];
		String cid = arr[3];
		
		if(map.containsKey(cid)){
			String cinfo = map.get(cid);
			String info = cinfo + "\t" + orderno + "\t" + price;
			System.out.println(info);
			context.write(new Text(info), NullWritable.get());
		}
		
	}

}
