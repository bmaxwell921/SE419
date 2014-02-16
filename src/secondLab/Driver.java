package secondLab;
/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 2 *********************
  * For question regarding this code,
  * please contact:
  * Srikanta Tirthapura (snt@iastate.edu)
  * Arko Provo Mukherjee (arko@iastate.edu)
  *****************************************
  *****************************************
  */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//TODO 
/*
 * It doesn't like that I changed the output of the second Map task
 */
public class Driver extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = "/datasets/Lab2/Gutenberg";    // Change this accordingly
		String temp = "/user/bmaxwell/Lab2/temp";      // Change this accordingly
		String output = "/user/bmaxwell/Lab2/output/";  // Change this accordingly
		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Driver Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputKeyClass(Text.class); 
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(IntWritable.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means wach map method recieves one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Driver.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(IntWritable.class);
		
		job_two.setMapOutputKeyClass(IntWritable.class);
		job_two.setMapOutputValueClass(Text.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp)); 
		FileOutputFormat.setOutputPath(job_two, new Path(output));
		
		// Run the job
		job_two.waitForCompletion(true); 
		return 0;
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method
	// However, to match the class declaration, it must emit Text as key and IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString().toLowerCase();
			
			List<String> sentences = this.splitSentences(line);

			List<String> curBigrams;
			for (String s: sentences) {
				curBigrams = this.makeBigrams(s);
				if (curBigrams != null) {
					for (String c: curBigrams) {
						context.write(new Text(c), new IntWritable(1));
					}
				}
				curBigrams = null;
			}
		}
		
		/*
		 * A method to split a line into sentences and remove extra punctuation
		 */
		private List<String> splitSentences(String in) {
			List<String> ret = new ArrayList<String>();
			StringBuilder sb = new StringBuilder();
			
			for (int i = 0; i < in.length(); ++i) {
				char cur = in.charAt(i);
				
				if (cur == '.' || cur == '!' || cur == '?') {
					if (!sb.toString().equals(" ")) {
						ret.add(sb.toString());
					}
					sb = new StringBuilder();
				} else if (Character.isLetter(cur) || cur == ' ') {
					sb.append(cur);
				}
			}
			ret.add(sb.toString());
			return ret;
		}
		
		/*
		 * Method to make bigrams from a given string.
		 * Output is in the form "word1, word2"
		 */
		private List<String> makeBigrams(String in) {
			if (in.length() == 0) return null;

			List<String> ret = new ArrayList<String>();
			StringTokenizer st = new StringTokenizer(in);
			
			if (!st.hasMoreTokens()) {
				return null;
			}
			String prev = st.nextToken();
			String cur = "";
			while (st.hasMoreTokens()) {
				cur = st.nextToken();
				ret.add(this.makeBigram(prev, cur));
				prev = cur;
			}
			return ret;
		}
		
		/*
		 * A method to make a bigram out of two given words
		 */
		private String makeBigram(String one, String two) {
			return one + ", " + two + " ";
		}
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
											throws IOException, InterruptedException  {
			
			int value = 0;
			for (IntWritable val : values) {
				value += val.get();
			}
			
			context.write(key, new IntWritable(value));			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, IntWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			context.write(new IntWritable(1), value);
			
			
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<IntWritable, Text, Text, IntWritable>  {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			BigramBox[] best = new BigramBox[10];
			int usedSpace = 0;
			
			for (Text t: values) {
				usedSpace = this.addBigram(best, usedSpace, new BigramBox(t.toString()));
			}
			
			for (BigramBox b: best) {
				context.write(new Text(b.bigram), new IntWritable(b.occurrences));
			}
			
		}  // End method "reduce"
		
		private int addBigram(BigramBox[] best, int usedSpace, BigramBox toAdd) {
			if (usedSpace >= best.length) {
				//Replace the smallest
				int min = this.findMin(best);
				if (best[min].compareTo(toAdd) < 0) {
					best[min] = toAdd;
				}
			} else {
				best[usedSpace++] = toAdd;
			}
			return usedSpace;
		}
		
		private int findMin(BigramBox[] box) {
			int min = 0;
			for (int i = 1; i < box.length; ++i) {
				if (box[min].compareTo(box[i]) > 0) {
					min = i;
				}
			}
			return min;
		}
		
		private static class BigramBox implements Comparable<BigramBox> {
			public String bigram;
			public int occurrences;
			
			public BigramBox(String big) {
				StringTokenizer st = new StringTokenizer(big);
				
				bigram = st.nextToken() + st.nextToken();
				occurrences = Integer.parseInt(st.nextToken());
			}
			
			public int compareTo(BigramBox rhs) {
				return this.occurrences - rhs.occurrences;
			}
		}
		
	}  // End Class Reduce_Two
	
	
	/**
	 * ******************************************************
	 * ******************************************************
	 * YOUR CODE HERE FOR MORE MAP / REDUCE CLASSES IF NEEDED
	 * ******************************************************
	 * ******************************************************
	 */
	
}