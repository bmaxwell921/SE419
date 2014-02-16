package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMap3 extends Mapper<LongWritable, Text, IntWritable, Text> {

	private static BufferedWriter bw;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/Map3Out.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Reducer2Out.txt"));
		
		List<String> ins = new ArrayList<String>();
		
		String read = "";
		
		while ((read = br.readLine()) != null) {
			ins.add(read);
		}
		
		TMap3 m = new TMap3();
		for (String s: ins) {
			m.map(new LongWritable(1), new Text(s), null);
		}
		
		bw.flush();
		bw.close();
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (context == null) {
			bw.write(value.toString());
			bw.newLine();
		} else {
			context.write(new IntWritable(1), value);
		}
	}
}
