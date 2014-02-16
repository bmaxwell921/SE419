package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TMap2 extends Mapper<LongWritable, Text, Text, Text> {
	
	private static BufferedWriter bw;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/Map2Out.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Reduce1Out.txt"));
		
		Map<String, String> values = new HashMap<String, String>();
		String read = "";
		
		while ((read = br.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(read);
			String key = st.nextToken();
			String value = getRestOfTokens(st);
			
			values.put(key, value);
		}
		
		TMap2 m = new TMap2();
		for (String k: values.keySet()) {
			m.map(new Text(k), new Text(values.get(k)), null);	
		}
		
		bw.flush();
		bw.close();
	}
	
	private static String getRestOfTokens(StringTokenizer st) {
		StringBuilder sb = new StringBuilder();
		
		while (st.hasMoreTokens()) {
			sb.append(st.nextToken());
			sb.append(" ");
		}
		
		return sb.toString();
	}
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append(key.toString());
		sb.append(" ");
		
		List<String> outs = this.createOuts(value.toString());
		
		for (String s: outs) {
			sb.append(s);
			sb.append(" ");
		}
		
		for (String s: outs) {
			if (context == null) {
				bw.write(s + "\t" + sb.toString());
				bw.newLine();
			} else {
				context.write(new Text(s), new Text(sb.toString()));
			}
		}
	}
	
	private List<String> createOuts(String s) {
		StringTokenizer st = new StringTokenizer(s);
		List<String> ret = new ArrayList<String>();
		
		while (st.hasMoreTokens()) {
			ret.add(st.nextToken());
		}
		
		return ret;
	}

}
