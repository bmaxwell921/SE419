package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMap1 extends Mapper<LongWritable, Text, Text, Text> {

	private static BufferedWriter bw;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/Map1Out.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Graph.txt"));
		
		ArrayList<Text> values = new ArrayList<Text>();
		String read = "";
		while ((read = br.readLine()) != null) {
			values.add(new Text(read));
		}
		
		TMap1 m = new TMap1();
		for (Text t: values) {
			m.map(new LongWritable(1), t, null);
		}
		
		bw.flush();
		bw.close();
	}
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		String keyOne = st.nextToken();
		String keyTwo = st.nextToken();
		
		if (context == null) {
			bw.write(keyOne + "\t" + keyTwo);
			bw.newLine();
			bw.write(keyTwo + "\t" + keyOne);
			bw.newLine();
		} else {
			context.write(new Text(keyOne), new Text(keyTwo));
			context.write(new Text(keyTwo), new Text(keyOne));
		}
	}
}
