package pku.edu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class step2 {
	
	public static class step2_MovieMatrixMap extends Mapper<Object, Text, Text, IntWritable>{

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
				System.out.println("input"+movieID1);
				for (int j = 0; j < valueStrings.length; j++) {
					String movieID2=valueStrings[j].split(":")[0];
					context.write(new Text(movieID1+":"+movieID2), new IntWritable(1));
				}
			}
			}
		}

		@Override
		protected void setup(
				Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		}
		
	}
	
	
	public static class step2_MovieMatrixReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			System.out.println("step2 reduce");
		      int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
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
	    job.setJarByClass(step2.class);
	    job.setMapperClass(step2_MovieMatrixMap.class);
	    job.setReducerClass(step2_MovieMatrixReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_1"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/step_out_2"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
