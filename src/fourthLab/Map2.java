package fourthLab;

import java.io.BufferedWriter;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

	private static BufferedWriter bw;
	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("src/fourthLab/Map2Out.txt"));
//		
//		ArrayList<String> values = MapReduceUtil.readMap("src/fourthLab/Reduce1Out.txt");
//		
//		Map2 m = new Map2();
//		for (String s: values) {
//			m.map(new LongWritable(1), new Text(s), null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (context == null) {
			bw.write(1 + "\t" + value.toString());
			bw.newLine();
		} else {
			System.out.println("Read: " + value.toString());
			context.write(new IntWritable(1), value);
		}
	}
}
