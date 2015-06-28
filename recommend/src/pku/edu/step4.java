package pku.edu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class step4 {
/*
	public static  int M_M=106;
	public static  int M_N=106;
	public static  int M_K=5;
	*/
	public static class matrixMap extends Mapper<Object, Text, Text, Text>{
		
		int M_M;
		int M_K;
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				M_M=context.getConfiguration().getInt("matrix_m", 100);
				M_K=context.getConfiguration().getInt("matrix_k", 100);
			} catch (Exception e) {
				// TODO: handle exception
			}
			
		}
		
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit intputSplit=context.getInputSplit();
			String name=((FileSplit)intputSplit).getPath().getParent().toString();
			String fileString=((FileSplit)intputSplit).getPath().getName().toString();
			
			if(name.contains("step_out_2")){
				if(fileString.contains("part")){
					
					String line=value.toString();
					
					String[] values=line.split("\t");
					//System.out.println(values.length);
					String row=values[0].split(":")[0];
					String col=values[0].split(":")[1];
					String val=values[1];
					
					
					if(Integer.parseInt(row)<=M_M && Integer.parseInt(col)<=M_M){
						//System.out.println(fileString+"map+A"+"====="+row);
						//System.out.println("a"+row+"+"+col+"+"+val);
						for(int i=1;i<=M_K;i++){
							context.write(new Text(row+" "+i), new Text("a"+" "+col+" "+val));
						}
					}
				}
			}
			else if(name.contains("step_out_3")){
				if(fileString.contains("part")){
					String line=value.toString();
					String[] values=line.split("\t");
					String row=values[0];
					String col=values[1].split(":")[0];
					
					String val=values[1].split(":")[1];
					System.out.println(val);
					//System.out.println(val);
					//val=String.valueOf(((int)Double.parseDouble(val)));
					//System.out.println("b"+val);
					if(Integer.parseInt(col)<=M_K && Integer.parseInt(row)<=M_M){
						//System.out.println(fileString+"map+B"+"====="+row);
						for(int i=1;i<=M_M;i++){
							context.write(new Text(i+" "+col), new Text("b"+" "+row+" "+val));
						}
					}

				}
			

			}

		
		}


	}
		
	public static class matrixReduce extends Reducer<Text, Text, Text, Text>{
		
		int M_M;
		int M_N;
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				M_M=context.getConfiguration().getInt("matrix_m", 100);
				M_N=context.getConfiguration().getInt("matrix_n", 100);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double [] valueA=new double[M_N];
			double [] valueB=new double[M_N];
			//System.out.println("reduce"+M_N);
			for(int i=0;i<M_N;i++){
				valueA[i]=0;
				valueB[i]=0;
			}

			for(Text subText:values){
				String valueString=subText.toString();
				if (valueString.startsWith("a")) {
					String [] splitStrings=valueString.split(" ");
					valueA[Integer.parseInt(splitStrings[1])-1]=Double.parseDouble(splitStrings[2]);
					
				}
				else if(valueString.startsWith("b")) {
					String [] splitStrings=valueString.split(" ");
					valueB[Integer.parseInt(splitStrings[1])-1]=Double.parseDouble(splitStrings[2]);
				}
			}
			double res=0;
			for (int i = 0; i < M_N; i++) {
				res+=valueA[i]*valueB[i];
			}
			//ju zhen  you  hua 
			if(res!=0){
				context.write(key, new Text(Double.toString(res)));
				//System.out.println(res);
			}
		}


		
	}
	
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    /*if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out> ");
	      System.exit(2);
	    }*/
	    conf.setInt("matrix_m",107 );
	    conf.setInt("matrix_n", 107);
	    conf.setInt("matrix_k", 5);
	    Job job = new Job(conf, "test");
	    job.setJarByClass(step4.class);
	    job.setMapperClass(matrixMap.class);
	    job.setReducerClass(matrixReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_2"));
	    FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/step_out_3"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/recom"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

