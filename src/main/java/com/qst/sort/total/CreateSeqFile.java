package com.qst.sort.total;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class CreateSeqFile {
	
	public static void main(String[] args) throws Exception{
		Random r = new Random();
		
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("D:/seq/2.seq");
		Writer w = SequenceFile.createWriter(fs, conf, p, Text.class,IntWritable.class);
		for( int i = 1 ; i <= 1000 ; i++ ){
			w.append(new Text("tom" + r.nextInt(500)),new IntWritable(i));
		}
		w.close();
	}
	
	
	
	
	
	
	

}
