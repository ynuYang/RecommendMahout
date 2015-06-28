package pku.edu;
import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

import pku.edu.step1.step1_UserMap;
import pku.edu.step1.step1_UserReduce;

public class step3 {
	
	
	public static class step3_UserMatrixMap extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit intputSplit=context.getInputSplit();
			String name=((FileSplit)intputSplit).getPath().getName().toString();
			if(name.contains("part")){
				String []temmStrings=value.toString().split("	");
				String [] valueStrings=temmStrings[1].split(",");
				for (int i = 0; i < valueStrings.length; i++) {

					String movieID1=valueStrings[i].split(":")[0];
					String scoreString=valueStrings[i].split(":")[1];
					System.out.println(movieID1);
					context.write(new Text(movieID1), new Text(temmStrings[0]+":"+scoreString));
				}
			}
		}
		
	}
	
	public static class step3_UserMatrixReduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			System.out.println("step3 reduce");
			// TODO Auto-generated method stub
			for (Text val : values) {
				context.write(key, val); 
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
	    }
	    */
	    Job job = new Job(conf, "test");
	    job.setJarByClass(step3.class);
	    job.setMapperClass(step3_UserMatrixMap.class);
	    job.setReducerClass(step3_UserMatrixReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_1"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/step_out_3"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
