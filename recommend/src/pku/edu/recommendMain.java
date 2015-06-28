package pku.edu;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pku.edu.step1.step1_UserMap;
import pku.edu.step1.step1_UserReduce;
import pku.edu.step2.step2_MovieMatrixMap;
import pku.edu.step2.step2_MovieMatrixReduce;
import pku.edu.step3.step3_UserMatrixMap;
import pku.edu.step3.step3_UserMatrixReduce;
import pku.edu.step4.matrixMap;
import pku.edu.step4.matrixReduce;
import pku.edu.step5.step5_matrixUseHDFSmap;
import pku.edu.step5.step5_matrixUseHDFSreduce;
import pku.edu.step6.step6_matrixUseHDFSmap;
import pku.edu.step6.step6_matrixUseHDFSreduce;

public class recommendMain {
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

	    Configuration conf = new Configuration();
	    conf.setInt("matrix_m",10000);
	    conf.setInt("matrix_n",10000);
	    conf.setInt("matrix_k",100000);
	    String HDFS="hdfs://master:9000";
	    String input_1=HDFS+"/input";
	    String output_1=HDFS+"/step_out_1";
	    String input_2=output_1;
	    String output_2=HDFS+"/step_out_2";

	    String input_3=output_1;
	    String output_3=HDFS+"/step_out_3";

	    String input_5_1=output_2;
	    String input_5_2=output_3;
	    String output_5=HDFS+"/step_out_5";
	    
	    String input_6=output_5;
	    String output_6=HDFS+"/step_out_6";

	    
	   	//work 1
	    Job job_step1 = new Job(conf, "step1");
	    job_step1.setJarByClass(step1.class);
	    
	    job_step1.setMapperClass(step1_UserMap.class);
	    job_step1.setReducerClass(step1_UserReduce.class);
	    
	    job_step1.setOutputKeyClass(Text.class);
	    job_step1.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job_step1, new Path(input_1));
	    FileOutputFormat.setOutputPath(job_step1, new Path(output_1));
	    ControlledJob ctrljob1=new  ControlledJob(conf); 
	    ctrljob1.setJob(job_step1);

	    //work2
	    Job job_step2 =new Job(conf, "step2");

	    job_step2.setJarByClass(step2.class);
	    
	    job_step2.setMapperClass(step2_MovieMatrixMap.class);
	    job_step2.setReducerClass(step2_MovieMatrixReduce.class);
	    
	    job_step2.setOutputKeyClass(Text.class);
	    job_step2.setOutputValueClass(IntWritable.class);
	    job_step2.setNumReduceTasks(5);
	    FileInputFormat.addInputPath(job_step2, new Path(input_2));
	    FileOutputFormat.setOutputPath(job_step2, new Path(output_2));
	    ControlledJob ctrljob2=new  ControlledJob(conf); 
	    ctrljob2.setJob(job_step2);
	    ctrljob2.addDependingJob(ctrljob1);

	    //work3

	    Job job_step3 =new Job(conf, "step3");

	    job_step3.setJarByClass(step3.class);
	    
	    job_step3.setMapperClass(step3_UserMatrixMap.class);
	    job_step3.setReducerClass(step3_UserMatrixReduce.class);
	    
	    job_step3.setOutputKeyClass(Text.class);
	    job_step3.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job_step3, new Path(input_3));
	    FileOutputFormat.setOutputPath(job_step3, new Path(output_3));
	    ControlledJob ctrljob3=new  ControlledJob(conf); 
	    ctrljob3.setJob(job_step3);
	    ctrljob3.addDependingJob(ctrljob2);


	    //work5
	    Job job_step5 =new Job(conf, "step5");

	    job_step5.setJarByClass(step5.class);
	    
	    job_step5.setMapperClass(step5_matrixUseHDFSmap.class);
	    job_step5.setReducerClass(step5_matrixUseHDFSreduce.class);
	    
	    job_step5.setOutputKeyClass(Text.class);
	    job_step5.setOutputValueClass(Text.class);
	    job_step5.setNumReduceTasks(10);
	    FileInputFormat.addInputPath(job_step5, new Path(input_5_1));
	    FileInputFormat.addInputPath(job_step5, new Path(input_5_2));
	    FileOutputFormat.setOutputPath(job_step5, new Path(output_5));
	    ControlledJob ctrljob5=new  ControlledJob(conf); 
	    ctrljob5.setJob(job_step5);
	    ctrljob5.addDependingJob(ctrljob3);
	    
	    
	    
	    
	    
	    
	    //work6

	    Job job_step6 =new Job(conf, "step6");

	    job_step6.setJarByClass(step6.class);
	    
	    job_step6.setMapperClass(step6_matrixUseHDFSmap.class);
	    job_step6.setReducerClass(step6_matrixUseHDFSreduce.class);
	    
	    job_step6.setOutputKeyClass(Text.class);
	    job_step6.setOutputValueClass(Text.class);
	    job_step6.setNumReduceTasks(5);
	    FileInputFormat.addInputPath(job_step6, new Path(input_6));
	    FileOutputFormat.setOutputPath(job_step6, new Path(output_6));
	    ControlledJob ctrljob6=new  ControlledJob(conf); 
	    ctrljob6.setJob(job_step6);
	    ctrljob6.addDependingJob(ctrljob5);
	    
	    

	    JobControl jobCtrl=new JobControl("myctrl"); 

	    jobCtrl.addJob(ctrljob1); 
	    jobCtrl.addJob(ctrljob2);
	    jobCtrl.addJob(ctrljob3);
	    jobCtrl.addJob(ctrljob5);  
	    jobCtrl.addJob(ctrljob6);
	    Thread  t=new Thread(jobCtrl); 
	    t.start(); 

	    while(true){ 
	
	    if(jobCtrl.allFinished()){ 
		    System.out.println(jobCtrl.getSuccessfulJobList()); 
		    jobCtrl.stop(); 
		    break; 
	    }
	   }

	  }
	}


