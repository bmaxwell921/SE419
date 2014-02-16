package common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

public class MapReduceUtil {

	public static ArrayList<String> readMap(String input) throws IOException {
		File in = new File(input);
		if (!in.exists()) {
			return null;
		}
		
		ArrayList<String> ret = new ArrayList<String>();
		
		BufferedReader br = new BufferedReader(new FileReader(in));
		String read = "";
		while ((read = br.readLine()) != null) {
			ret.add(read);
		}
		
		br.close();
		return ret;
	}
	
	public static Map<String, ArrayList<String>> readReduce(String input) throws IOException {
		File in = new File(input);
		
		if (!in.exists()) {
			return null;
		}
		
		Map<String, ArrayList<String>> ret = new HashMap<String, ArrayList<String>>();
		
		BufferedReader br = new BufferedReader(new FileReader(in));
		String read = "";
		while((read = br.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(read);
			String key = st.nextToken();
			String value = st.nextToken();
			
			ArrayList<String> curValues = ret.get(key);
			if (curValues == null) {
				curValues = new ArrayList<String>();
			}
			curValues.add(value);
			ret.put(key, curValues);
		}
		
		br.close();
		return ret;
	}
	
	public static Map<String, ArrayList<Text>> convertReduceMapToText(Map<String, ArrayList<String>> values) {
		Map<String, ArrayList<Text>> ret = new HashMap<String, ArrayList<Text>>();
		for (String key : values.keySet()) {
			ret.put(key, stringListToTextList(values.get(key)));
		}
		
		return ret;
	}
	
	public static ArrayList<Text> stringListToTextList(ArrayList<String> values) {
		ArrayList<Text> vals = new ArrayList<Text>();
		for (String s: values) {
			vals.add(new Text(s));
		}
		
		return vals;
	}
}
