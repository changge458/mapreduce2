package com.qst.kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Utils {
    
    /**
     * 读取中心文件的数据
     * 将所有文件(包括文件夹内部的数据递归读取)，以数组嵌套方式返回
     * @param centersPath
     * @param isDirectory
     * @return
     * @throws Exception
     */
public static ArrayList<ArrayList<Double>> getCentersFromHDFS(String centersPath,boolean isDirectory) throws IOException{
        
        ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
        
        Path path = new Path(centersPath);
        
        Configuration conf = new Configuration();
        
        FileSystem fileSystem = path.getFileSystem(conf);

        if(isDirectory){    
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (int i = 0; i < listFile.length; i++) {
                result.addAll(getCentersFromHDFS(listFile[i].getPath().toString(),false));
            }
            return result;
        }
        
        FSDataInputStream fsis = fileSystem.open(path);
        LineReader lineReader = new LineReader(fsis, conf);
        
        Text line = new Text();
        
        while(lineReader.readLine(line) > 0){
            ArrayList<Double> tempList = textToArray(line);
            result.add(tempList);
        }
        lineReader.close();
        return result;
    }
    
    //删掉文件
    public static void deletePath(String pathStr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path ,true);
    }
    
    public static ArrayList<Double> textToArray(Text text){
        ArrayList<Double> list = new ArrayList<Double>();
        String[] fileds = text.toString().split(",");
        for(int i=0;i<fileds.length;i++){
            list.add(Double.parseDouble(fileds[i]));
        }
        return list;
    }
    
    /**
     * 核心比较代码
     * @param centerPath
     * @param newPath
     * @return
     * @throws IOException
     */
    public static boolean compareCenters(String centerPath,String newPath) throws IOException{
        
        List<ArrayList<Double>> oldCenters = Utils.getCentersFromHDFS(centerPath,false);
        List<ArrayList<Double>> newCenters = Utils.getCentersFromHDFS(newPath,true);
        
        int size = oldCenters.size();
        int fieldSize = oldCenters.get(0).size();
        double distance = 0;
        for(int i=0;i<size;i++){
            for(int j=0;j<fieldSize;j++){
                double t1 = oldCenters.get(i).get(j);
                double t2 = newCenters.get(i).get(j);
                distance += Math.pow(t1 - t2 , 2);
            }
        }
        
        if(distance == 0.0){
            //删掉新的中心文件以便最后依次归类输出
            Utils.deletePath(newPath);
            return true;
        }else{
        	
            //清空原中心文件
            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);
            
            FSDataOutputStream overWrite = fileSystem.create(outPath,true);
            overWrite.writeChars("");
            overWrite.close();
            
            //将新产生的中心文件写入到原中心文件的路径
            Path inPath = new Path(newPath);
            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (int i = 0; i < listFiles.length; i++) {                
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            //删掉新的中心文件以便第二次任务运行输出
            Utils.deletePath(newPath);
        }
        
        return false;
    }
}