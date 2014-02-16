package thirdLab.exp1;

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

public class GraphSignificance extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GraphSignificance(), args);
		System.exit(res);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		String graphPath = "/datasets/Lab3/Graph";
		String output = "/user/bmaxwell/Lab3/exp1/output";
		String MR1Out = "/user/bmaxwell/Lab3/exp1/MR1Out";
		String MR2Out = "/user/bmaxwell/Lab3/exp1/MR2Out";
		
		Configuration conf = new Configuration();
		
		//Round one stuff
		
		Job jobOne = new Job(conf, "Job One");
		this.setUpJob(jobOne, GraphSignificance.class, 2, Text.class, 
				Text.class, Text.class, Text.class, 
				Map1.class, Reduce1.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				graphPath, MR1Out, true);
		
		Job jobTwo = new Job(conf, "Job Two");
		this.setUpJob(jobTwo, GraphSignificance.class, 2, Text.class, 
				IntWritable.class, Text.class, Text.class,
				Map2.class, Reduce2.class, 
				TextInputFormat.class, TextOutputFormat.class, 
				MR1Out, MR2Out, true);
		
		Job jobThree = new Job(conf, "Job Three");
		this.setUpJob(jobThree, GraphSignificance.class, 2, Text.class, 
				IntWritable.class, IntWritable.class, Text.class, 
				Map3.class, Reduce3.class, 
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
