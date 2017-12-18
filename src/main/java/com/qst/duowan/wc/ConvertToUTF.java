package com.qst.duowan.wc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

public class ConvertToUTF {
	public static void main(String[] args) throws Exception {
		InputStreamReader isr = new InputStreamReader(new FileInputStream("D:/wc/duowan_user.txt"), "GBK");

		OutputStreamWriter osr = new OutputStreamWriter(new FileOutputStream("D:/wc/duowan_user2.txt"), "utf-8");
		
		char[] buf = new char[1024];
		int len = 1024;
		
		while((len = isr.read(buf)) != -1 ){
			osr.write(buf, 0, len);
		}
		isr.close();
		osr.close();
		
	}

}
