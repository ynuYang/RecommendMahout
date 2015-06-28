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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class step5 {

	public static class step5_matrixUseHDFSmap extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit intputSplit=context.getInputSplit();
			String name=((FileSplit)intputSplit).getPath().getParent().toString();
			String fileString=((FileSplit)intputSplit).getPath().getName().toString();
			String[] valuesplit= value.toString().split("\t");
			//tong xian ju zhen
			System.out.println(valuesplit.length);
			if(name.contains("step_out_2")){
				if(fileString.contains("part")){
					System.out.println("step5.step5_matrixUseHDFSmap 222");
					String [] items=valuesplit[0].split("\\:");
					String itemID1 = items[0];
	                String itemID2 = items[1];
	                String num = valuesplit[1]; 
	                Text k = new Text(itemID1);
	                Text v = new Text("A:" + itemID2 + "," + num);
	                context.write(k, v);
				}
			}
			//ping fen ju zhen
			else if(name.contains("step_out_3")){
				if(fileString.contains("part")){
					System.out.println("step5.step5_matrixUseHDFSmap333");
					String[] items=valuesplit[1].split("\\:");
					String itemID = valuesplit[0];
	                String userID = items[0];
	                String pref = items[1];

	                Text k = new Text(itemID);
	                Text v = new Text("B:" + userID + "," + pref);

	                context.write(k, v);
				}
			

			}
		}

		
	}
	
	
	public static class step5_matrixUseHDFSreduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			Map<String, String> mapA=new HashMap<String, String>();
			Map<String, String> mapB=new HashMap<String, String>();
			for (Text text : values) {
	            String val = text.toString();
               System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = val.substring(2).split("\\,");
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv =val.substring(2).split("\\,");
                    mapB.put(kv[0], kv[1]);

                } 
			}
			
	         double result = 0;
	            Iterator iter = mapA.keySet().iterator();
	            while (iter.hasNext()) {
	                String mapk = (String) iter.next();// itemID

	                int num = Integer.parseInt(mapA.get(mapk));
	                Iterator iterb = mapB.keySet().iterator();
	                while (iterb.hasNext()) {
	                    String mapkb = (String) iterb.next();// userID
	                    double pref = Double.parseDouble(mapB.get(mapkb));
	                    result = num * pref;// 矩阵乘法相乘计算

	                    Text k = new Text(mapkb);
	                    Text v = new Text(mapk + "," + result);
	                    context.write(k, v);
	                    System.out.println(k.toString() + "  " + v.toString());
	                }
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
	    job.setJarByClass(step5.class);
	    job.setMapperClass(step5_matrixUseHDFSmap.class);
	    job.setReducerClass(step5_matrixUseHDFSreduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_2"));
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_3"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/step_out_5"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
