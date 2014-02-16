package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, Text> {
	
//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Map1Out.txt"));
//		
//		List<Text> values = new ArrayList<Text>();
//		BufferedReader br = new BufferedReader(new FileReader("Map1In.txt"));
//		
//		String read = "";
//		while ((read = br.readLine()) != null) {
//			values.add(new Text(read));
//		}
//		
//		Map1 m = new Map1();
//		for (Text t: values) {
//			m.map(new LongWritable(1), t, null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		
		String fromVertex = st.nextToken();
		String toVertex = st.nextToken();
		
		if (context == null) {
//			bw.write(toVertex + "\tIn:" + fromVertex);
//			bw.newLine();
//			bw.write(fromVertex + "\tOut:" + toVertex);
//			bw.newLine();
		} else {
			//Send it out as "In:A"
			context.write(new Text(toVertex), new Text("In:" + fromVertex));
			
			//Send it out as "Out:B"
			context.write(new Text(fromVertex), new Text("Out:" + toVertex));
		}
	}
}
