package thirdLab.exp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class Triangles extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Triangles(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		String graphPath = "/datasets/Lab3/Graph";
		String output = "/user/bmaxwell/Lab3/exp2/output";
		String MR1Out = "/user/bmaxwell/Lab3/exp2/MR1Out";
		String MR2Out = "/user/bmaxwell/Lab3/exp2/MR2Out";
		
		Configuration conf = new Configuration();
		
		Job one = new Job(conf, "Job One");
		this.setUpJob(one, Triangles.class, 2, Text.class, 
				Text.class, Text.class, Text.class, 
				TMap1.class, TReduce1.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				graphPath, MR1Out, true);
		
		Job two = new Job(conf, "Job Two");
		this.setUpJob(two, Triangles.class, 2, Text.class, 
				Text.class, Text.class, Text.class, 
				TMap2.class, TReduce2.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				MR1Out, MR2Out, true);
		
		Job three = new Job(conf, "Job Three");
		this.setUpJob(three, Triangles.class, 2, LongWritable.class, 
				Text.class, IntWritable.class, Text.class, 
				TMap3.class, TReduce3.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				MR2Out, output, true);
		
		
		return 0;
	}
	
	private void setUpJob(Job job, Class<?> jarClass, int reduceTasks, Class<?> outputReducerKeyClass, 
			Class<?> outputReducerValueClass, Class<?> outputMapKeyClass, Class<?> outputMapValueClass,
			Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass, 
			Class<? extends InputFormat> inputFormatClass, Class<? extends OutputFormat> outputFormatClass, 
			String inputPath, String outputPath, boolean waitForComp) throws Exception {
		//Attach the job to its runner
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

}
