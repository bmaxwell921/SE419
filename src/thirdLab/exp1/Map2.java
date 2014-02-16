package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map2 extends Mapper<LongWritable, Text, Text, Text> {
	
//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Map2Out.txt"));
//		BufferedReader br = new BufferedReader(new FileReader("Reduce1Out.txt"));
//		
//		List<Text> values = new ArrayList<Text>();
//		String read = "";
//		
//		while ((read = br.readLine()) != null) {
//			values.add(new Text(read));
//		}
//		
//		Map2 m = new Map2();
//		for (Text t: values) {
//			m.map(new LongWritable(1), t, null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
		//	Value comes in as
		// 	A	B,C;F,G
		StringTokenizer st = new StringTokenizer(value.toString());
		
		String outKey = st.nextToken();
		Set<String> outVals = new HashSet<String>();
		Set<String> inVals = new HashSet<String>();
		
		inVals.add(outKey);
		
		this.fillInAndOut(st.nextToken(), inVals, outVals);
		
		// From the above example we want to send out these values:
		// (F,3), (G,3)
		for (String out: outVals) {
			for (String in: inVals) {
				if (context == null) {	
//					bw.write(out + "\t" + in);
//					bw.newLine();
				} else {
					//context.write(new IntWritable(1), new Text(out + "," + in));
					context.write(new Text(out), new Text(in));
				}
			}
		}
	}

	private void fillInAndOut(String value, Set<String> inVals, Set<String> outVals) {
		// Value looks like B,C;F,G
		String[] insOuts = value.split(";");
		
		if (insOuts.length >= 1) {
			this.fillList(insOuts[0], inVals);
		}
		if (insOuts.length >= 2) {
			this.fillList(insOuts[1], outVals);
		}
	}

	private void fillList(String string, Set<String> set) {
		// String looks like: 
		// B,C
		StringTokenizer st = new StringTokenizer(string, ",");
		while (st.hasMoreTokens()) {
			set.add(st.nextToken());
		}
	}
}