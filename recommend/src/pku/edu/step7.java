package pku.edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class step7 {
	public static class MyPairWritable implements WritableComparable<MyPairWritable>
	{
		IntWritable first;
		IntWritable second;
		DoubleWritable third;
		
		public void set(IntWritable first,IntWritable second,DoubleWritable third)
		{
			this.first=first;
			this.second=second;
			this.third=third;
		}
		
		public IntWritable getFirst()
		{
			return first;
		}
		
		public IntWritable getSecond()
		{
			return second;
		}
		
		public DoubleWritable getThird()
		{
			return third;
		}
		
		public void readFields(DataInput in) throws IOException
		{
			first=new IntWritable(in.readInt());
			second=new IntWritable(in.readInt());
			third=new DoubleWritable(in.readDouble());
		}
		
		public void write(DataOutput out)throws IOException
		{
			out.writeInt(first.get());
			out.writeInt(second.get());
			out.writeDouble(third.get());
		}
		@Override
		public int compareTo(MyPairWritable o)
		{
			if(this.first!=o.getFirst())
			{
				return this.first.get()-o.getFirst().get();
			}else {
				if(this.third.get()-o.getThird().get()>0)
				{
					return -1;
				}else if(this.third.get()-o.getThird().get()<0)
				{
					return 1;
				}else
				{
					return 0;
				}
			}
		}
		@Override
		public int hashCode()
		{
			return first.hashCode()*167+second.hashCode()*10+third.hashCode();
			
		}
		
		@Override
		public boolean equals(Object obj)
		{
			MyPairWritable temp=(MyPairWritable) obj;
			return first.equals(temp.first) && second.equals(temp.second) && third.equals(temp.third);
		}
		
		@Override
		public String toString()
		{
			return first.get()+" "+second.get()+" "+third.get();
		}
		
	}
	
	public static class FirstPartitioner extends Partitioner<MyPairWritable,IntWritable>
	{
		public int getPartition(MyPairWritable key,IntWritable value,int numPartitions)
		{
			return Math.abs(key.getFirst().get()*167)%numPartitions;
		}
	}
	
	
	public static class GroupingComparator extends WritableComparator
	{
		protected GroupingComparator()
		{
			super(MyPairWritable.class,true);
		}
		
		@Override
		public int compare(WritableComparable w1,WritableComparable w2)
		{
			MyPairWritable p1=(MyPairWritable) w1;
			MyPairWritable p2=(MyPairWritable) w2;
			int l=p1.getFirst().get();
			int r=p2.getFirst().get();
			return 1==r ? 0:(l<r ? -1:1);
		}
	}
	
	public static class KeyComparator extends WritableComparator
	{
		protected KeyComparator()
		{
			super(MyPairWritable.class,true);
		}
		
		@Override
		public int compare(WritableComparable a,WritableComparable b)
		{
			MyPairWritable p1=(MyPairWritable) a;
			MyPairWritable p2=(MyPairWritable) b;
			if(!p1.getFirst().equals(p2.getFirst()))
			{
				return p1.getFirst().get()-p2.getFirst().get();
			}else
			{
				if(p1.third.get()-p2.getThird().get()>0)
				{
					return -1;
				}else if(p1.third.get()-p2.getThird().get()<0)
				{
					return 1;
				}else
				{
					return 0;
				}
			}
			
		}
	}
	

	public static class step7_map extends Mapper<LongWritable,Text,MyPairWritable,NullWritable>
	{
		MyPairWritable intkey=new MyPairWritable();
		
		protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			InputSplit intputSplit=context.getInputSplit();
			String name=((FileSplit)intputSplit).getPath().getName().toString();
			if(name.contains("part")){
			String[] line=value.toString().split("\t");
	
			int intone=Integer.parseInt(line[0]);
			String[] num=line[1].toString().split(",");
			int inttwo=Integer.parseInt(num[0]);
			double intthree=Double.parseDouble(num[1]);
//			StringTokenizer tokenizer=new StringTokenizer(line);
////			if(tokenizer.hasMoreTokens())
////			{
////				intone=Integer.parseInt(tokenizer.nextToken());
////				if(tokenizer.hasMoreTokens())
////				{
////					inttwo=Integer.parseInt(tokenizer.nextToken());
////					if(tokenizer.hasMoreTokens())
////						intthree=Integer.parseInt(tokenizer.nextToken());
////					
////				}
////			}
//			
			IntWritable one=new IntWritable(intone);
			IntWritable two=new IntWritable(inttwo);
			DoubleWritable three=new DoubleWritable(intthree);
			
//				System.out.println("===============================");
//				System.out.println(value);
//				System.out.println(one+" "+two+" "+three);
//				System.out.println("===========");
//			

			intkey.set(one,two,three);
			System.out.println(intkey);
			context.write(intkey, NullWritable.get());
			}
		}
	}
	
	public static class step7_reduce extends Reducer<MyPairWritable,NullWritable,MyPairWritable,NullWritable>
	{
		//Text left=new Text();
		//Text SEPARATOR=new Text("===================================");
		@Override
		protected void reduce(MyPairWritable key,Iterable<NullWritable> values,Context context)throws IOException,InterruptedException
		{
			
			context.write(key, NullWritable.get());
//			System.out.println(key.toString());
		}
	}
	
	public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException
	{
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		Job job=new Job(conf,"secondarySort");
		job.setJarByClass(step7.class);
		
		job.setMapperClass(step7_map.class);
		job.setReducerClass(step7_reduce.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);		
		job.setOutputKeyClass(MyPairWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
