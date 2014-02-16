package thirdLab.exp1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce1 extends Reducer<Text, Text, Text, Text>{

//	private static BufferedWriter bw;
//	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("Reduce1Out.txt"));
//		
//		Map<String, ArrayList<Text>> reduce1In = new HashMap<String, ArrayList<Text>>();
//		BufferedReader br = new BufferedReader(new FileReader(new File("Map1Out.txt")));
//		String read = "";
//		while ((read = br.readLine()) != null) {
//			StringTokenizer st = new StringTokenizer(read);
//			String key = st.nextToken();
//			String value = st.nextToken();
//			
//			ArrayList<Text> values = reduce1In.get(key);
//			if (values == null) {
//				values = new ArrayList<Text>();
//				values.add(new Text(value));
//				reduce1In.put(key, values);
//			} else {
//				values.add(new Text(value));
//			}
//			
//		} 		
//		Reduce1 r = new Reduce1();
//		for (String k: reduce1In.keySet()) {
//			r.reduce(new Text(k), reduce1In.get(k), null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		
		Set<String> inNodes = new HashSet<String>();
		Set<String> outNodes = new HashSet<String>();
		
		// Values looks like:
		// {In:A, In:B, Out:C, Out:D}
		for (Text t: values) {
			// Puts the node id into either the in or the out
			// list depending on whether it has "in" or "out" in the value 
			this.addToCorrectList(t.toString(), inNodes, outNodes);
		}
		if (context == null) {
//			bw.write(key + "\t" + this.setsToString(inNodes, outNodes));
//			bw.newLine();
		} else {
			context.write(key, new Text(this.setsToString(inNodes, outNodes)));
		}
	}

	private void addToCorrectList(String string, Set<String> inNodes,
			Set<String> outNodes) {
		
		// String looks like In:A
		StringTokenizer st = new StringTokenizer(string);
		
		// The string comes in as "In:987" so if the first token (In:)
		// contains the word "In" we need to add the second token (987)
		// to the set of inNodes
		String tok = st.nextToken(":");
		Set<String> addTo = (tok.contains("In")) ? inNodes : outNodes;
		
		addTo.add(st.nextToken());
	}
	

	private String setsToString(Set<String> inNodes, Set<String> outNodes) {
		StringBuilder sb = new StringBuilder();
		
		// We want the output string to look like this:
		// A,B,C,D;F,G
		int count = 0;
		for (String s: inNodes) {
			sb.append(s);
			if (++count < inNodes.size()) {
				sb.append(",");
			}
		}
		
		sb.append(";");
		count = 0;
		for (String s: outNodes) {
			sb.append(s);
			if (++count < outNodes.size()) {
				sb.append(",");
			}
		}
		
		return sb.toString();
	}
}
