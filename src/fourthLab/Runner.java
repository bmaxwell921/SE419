package fourthLab;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Runner extends Configured implements Tool{


	public static void main ( String[] args ) throws Exception {

		int res = ToolRunner.run(new Configuration(), new Runner(), args);
		System.exit(res); 

	} // End main

	public int run ( String[] args ) throws Exception {
		
		/**
		 * You are not required to use the exact Mapping or Reducer layout or numbers as given in this file.
		 * Fill free to add or remove rounds of MapReduce as you need. This is just to serve as a starting 
		 * location.
		 */		

		String input = "/datasets/Lab4/test-files";
		String output = "/user/bmaxwell/Lab4/exp1/output";
		
		String MR1Out = "/user/bmaxwell/Lab4/exp1/MR1Out";
	   /*
		* Create your new jobs here as you've done in previous labs.
		*/
		
		Configuration conf = new Configuration();
		
		Job one = new Job(conf, "Job (D)one");
		this.setUpJob(one, Runner.class, 2, 
				Text.class, Text.class, //Reducer output key, value
				Text.class, Text.class, //Mapper output key, value
				Map1.class, Reduce1.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				input, MR1Out, true);
		
		Job two = new Job(conf, "Job two");
		this.setUpJob(two, Runner.class, 2, 
				Text.class, Text.class, //Reducer output key, value
				IntWritable.class, Text.class, //Mapper output key, value
				Map2.class, Reduce2.class,
				TextInputFormat.class, TextOutputFormat.class,
				MR1Out, output, true);
	
		return 0;

	} // End run
	
	private void setUpJob(Job job, Class<?> jarClass, int reduceTasks, Class<?> outputReducerKeyClass, 
			Class<?> outputReducerValueClass, Class<?> outputMapKeyClass, Class<?> outputMapValueClass,
			Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, 
			Class<? extends InputFormat> inputFormatClass, Class<? extends OutputFormat> outputFormatClass, 
			String inputPath, String outputPath, boolean waitForComp) throws Exception {
		
		job.setJarByClass(jarClass);
		
		job.setNumReduceTasks(reduceTasks);
		
		job.setOutputKeyClass(outputReducerKeyClass);
		job.setOutputValueClass(outputReducerValueClass);
		
		job.setMapOutputKeyClass(outputMapKeyClass);
		job.setMapOutputValueClass(outputMapValueClass);
		
		job.setMapperClass(mapperClass);
		job.setReducerClass(reducerClass);
		
		job.setInputFormatClass(inputFormatClass);
		job.setOutputFormatClass(outputFormatClass);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(waitForComp);
	}

/*
 * Below is a hash function that can be used throughout your Application
 * to help with minhashing. New hash functions can be derived by using a 
 * new seed value.
 */
	public static int hash(byte[] b_con, int i_seed){

		String content = new String(b_con);

		int seed = i_seed;
		int m = 0x5bd1e995;
		int r = 24;

		int len = content.length();
		byte[] work_array = null;

		int h = seed ^ content.length();

		int offset = 0;

		while( len >= 4)
		{
			work_array = new byte[4];
			ByteBuffer buf = ByteBuffer.wrap(content.substring(offset, offset + 4).getBytes());

			int k = buf.getInt();
			k = k * m;
			k ^= k >> r;
		k *= m;
		h *= m;
		h ^= k;

		offset += 4;
		len -= 4;
		}

		switch(len){
		case 3: h ^= work_array[2] << 16;
		case 2: h ^= work_array[1] << 8;
		case 1: h ^= work_array[0];
		h *= m;
		}

		h ^= h >> 13;
		h *= m;
		h ^= h >> 15;

		return h;
	}
}