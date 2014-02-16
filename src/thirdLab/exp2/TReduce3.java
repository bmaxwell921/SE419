package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReduce3 extends Reducer<IntWritable, Text, LongWritable, Text>{

	private static BufferedWriter bw;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/output.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Map3Out.txt"));
		
		List<Text> ins = new ArrayList<Text>();
		String read = "";
		
		while ((read = br.readLine()) != null) {
			ins.add(new Text(read));
		}
		
		TReduce3 r = new TReduce3();
		
		r.reduce(new IntWritable(1), ins, null);
		
		bw.flush();
		bw.close();
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//The values are already sorted so we can just throw them in a set to get the uniques
		
		Set<String> uniques = new HashSet<String>();
		for (Text t: values) {
			uniques.add(t.toString().trim());
		}
		
		if (context == null) {
			bw.write("" + uniques.size());
		} else {
			context.write(new LongWritable(uniques.size()), new Text(""));
		}
	}
}
