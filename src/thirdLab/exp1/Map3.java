package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map3 extends Mapper<LongWritable, Text, IntWritable, Text>{

//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Map3Out.txt"));
//		BufferedReader br = new BufferedReader(new FileReader("Reduce2Out.txt"));
//		
//		ArrayList<Text> values = new ArrayList<Text>();
//		String read = "";
//		while ((read = br.readLine()) != null) {
//			values.add(new Text(read));
//		}
//		
//		Map3 m = new Map3();
//		for (Text s: values) {
//			m.map(new LongWritable(1), s, null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (context == null) {
//			bw.write(value.toString());
//			bw.newLine();
		} else {
			context.write(new IntWritable(1), value);
		}
	}
}
