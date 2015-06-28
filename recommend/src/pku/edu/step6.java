package pku.edu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pku.edu.step5.step5_matrixUseHDFSmap;
import pku.edu.step5.step5_matrixUseHDFSreduce;

public class step6 {
	
	public static class step6_matrixUseHDFSmap extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] valuesplit= value.toString().split("\t");
			Text k = new Text(valuesplit[0]);
            Text v = new Text(valuesplit[1]);
            context.write(k, v);
		}

		
	}
	
	
	public static class step6_matrixUseHDFSreduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			Map<String, Double> map=new HashMap<String, Double>();
	        for (Text line : values) {
                
                String[] tokens = line.toString().split("\\,");
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                
                 if (map.containsKey(itemID)) {
                     map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
                 } else {
                     map.put(itemID, score);
                 }
            }
            
            Iterator iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next().toString();
                double score = map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }
		}
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out> ");
	      System.exit(2);
	    }*/
	    
	    Job job = new Job(conf, "test");
	    job.setJarByClass(step6.class);
	    job.setMapperClass(step6_matrixUseHDFSmap.class);
	    job.setReducerClass(step6_matrixUseHDFSreduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   // FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_2"));
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_5"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
