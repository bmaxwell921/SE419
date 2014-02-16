package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReduce1 extends Reducer<Text, Text, Text, Text>{

	private static BufferedWriter bw; 
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/Reduce1Out.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Map1Out.txt"));
		
		Map<String, List<Text>> values = new HashMap<String, List<Text>>();
		String read = "";
		
		while ((read = br.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(read);
			String key = st.nextToken();
			String value = st.nextToken();
			
			List<Text> vals = values.get(key);
			if (vals == null) {
				vals = new ArrayList<Text>();
			}
			vals.add(new Text(value));
			values.put(key, vals);
		}
		
		TReduce1 r = new TReduce1();
		for (String key: values.keySet()) {
			r.reduce(new Text(key), values.get(key), null);
		}
		
		bw.flush();
		bw.close();		
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		
		for (Text t: values) {
			sb.append(t.toString());
			sb.append(" ");
		}
		
		if (context == null) {
			bw.write(key.toString() + "\t" + sb.toString());
			bw.newLine();
		} else {
			context.write(key, new Text(sb.toString()));
		}
	}
}
