package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce2 extends Reducer<Text, Text, Text, IntWritable>{
	
//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Reduce2Out.txt"));
//		BufferedReader br = new BufferedReader(new FileReader("Map2Out.txt"));
//		
//		Map<String, ArrayList<Text>> vals = new HashMap<String, ArrayList<Text>>();
//		
//		String read = "";
//		while ((read = br.readLine()) != null) {
//			StringTokenizer st = new StringTokenizer(read);
//			String key = st.nextToken();
//			String value = st.nextToken();
//			
//			ArrayList<Text> values = vals.get(key);
//			if (values == null) {
//				values = new ArrayList<Text>();
//			}
//			values.add(new Text(value));
//			vals.put(key, values);
//		}
//		
//		Reduce2 r = new Reduce2();
//		for (String k: vals.keySet()) {
//			r.reduce(new Text(k), vals.get(k), null);
//		}
//				
//		bw.flush();
//		bw.close();
//	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<Text> uniqueValues = new HashSet<Text>();
		
		for (Text t: values) {
			uniqueValues.add(t);
		}
		
		if (context == null) {
//			bw.write(key.toString() + "\t" + uniqueValues.size());
//			bw.newLine();
		} else {
			context.write(key, new IntWritable(uniqueValues.size()));
		}
	}
	
//	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
//		throws IOException, InterruptedException {
//		// Values comes in as:
//		// {A,3 , B,3} 
//		Map<String, Set<String>> keyVals = new HashMap<String, Set<String>>();
//		
//		for (Text t: values) {
//			StringTokenizer st = new StringTokenizer(t.toString(), ",");
//			String k = st.nextToken();
//			String v = st.nextToken();
//			
//			Set<String> vals = keyVals.get(k);
//			if (vals == null) {
//				vals = new HashSet<String>();
//			} 
//			vals.add(v);
//			keyVals.put(k, vals);
//		}
//
//		NodeBox[] arr = new NodeBox[10];
//		int curOccupants = 0;
//		
//		for (String k: keyVals.keySet()) {
//			curOccupants = this.addElement(arr, new NodeBox(k, keyVals.get(k).size()), curOccupants);
//		}
//		for (NodeBox nb: arr) {
//			if (context == null) {
//				if (nb != null) {
////					bw.write(nb.node + "\t" + nb.significance);
////					bw.newLine();
//				}
//			} else {
//				context.write(new Text(nb.node), new IntWritable(nb.significance));
//			}
//		}
//	}
}
