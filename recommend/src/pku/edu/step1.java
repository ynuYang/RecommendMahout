package pku.edu;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class step1 {

	public static class step1_UserMap extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String []temmStrings=value.toString().split(" ");
			System.out.println(temmStrings.length);
			if(temmStrings.length==4){
				String UserID=temmStrings[0];
				String MoiveID=temmStrings[1];
				String socore=temmStrings[3];
				context.write(new Text(UserID),new Text(MoiveID+":"+socore));
			}
		}
		
	}
	
	
	public static class step1_UserReduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String reString="";
			for(Text value:values){
				reString=reString+","+value.toString();
			}
			reString=reString.replaceFirst(",","");
			context.write(key, new Text(reString));
		}
		
	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out> ");
	      System.exit(2);
	    }*/
	    
	    Job job = new Job(conf, "test");
	    job.setJarByClass(step1.class);
	    job.setMapperClass(step1_UserMap.class);
	    job.setReducerClass(step1_UserReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/input"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/step_output_11"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
}
