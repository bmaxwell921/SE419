package secondLabTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class MapOneTest {

	public List<String> getText() {
		List<String> ret = new ArrayList<String>();
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("src/secondLabTest/input")));
			String in = "";
			while ((in = br.readLine()) != null) {
				ret.add(in);
			}
		} catch (IOException ioe) {
			System.out.println("THREW EXCEPTION!");
			throw new RuntimeException();
		}
		return ret;
	}
	
	public static void main(String[] args) {
		MapOneTest mot = new MapOneTest();
		List<String> test = mot.getText();

		Map<String, List<Integer>> bigrams = new HashMap<String, List<Integer>>();
		for (String s: test) {
			Map<String, List<Integer>> temp = mot.map(s);
			
			for (String key: temp.keySet()) {
				List<Integer> bigramsTotal = bigrams.get(key);
				List<Integer> bigramsParti = temp.get(key);
				
				if (bigramsTotal == null) {
					bigramsTotal = new ArrayList<Integer>();
					bigramsTotal.addAll(bigramsParti);
				} else {
					bigramsTotal.addAll(bigramsParti);
				}
				bigrams.put(key, bigramsTotal);
			}
		}
		
		List<String> reduceOutput = new ArrayList<String>();
		for (String key : bigrams.keySet()) {
			reduceOutput.add(mot.reduce(key, bigrams.get(key)));
		}
		
		List<String> map2Output = new ArrayList<String>();
		for (String s: reduceOutput) {
			map2Output.add(mot.map(2l, s));
		}
		
		List<String> output = mot.reduce2(map2Output);
		
		for (String s: output) {
			System.out.println(s);
		}
	}

	public Map<String, List<Integer>> map(String line) {
		Map<String, List<Integer>> ret = new HashMap<String, List<Integer>>();

		line = line.toLowerCase();

		List<String> sentences = this.splitSentences(line);

		List<String> bigrams = new ArrayList<String>();

		for (String s : sentences) {
			List<String> curBigrams = this.makeBigrams(s);
			if (curBigrams != null) {
				bigrams.addAll(curBigrams);
			}
		}

		for (String s : bigrams) {
			List<Integer> values = ret.get(s);

			if (values != null) {
				values.add(1);
			} else {
				values = new ArrayList<Integer>();
				values.add(1);
			}
			ret.put(s, values);
		}

		return ret;
	}

	/*
	 * A method to split a line into sentences and remove extra punctuation
	 */
	private List<String> splitSentences(String in) {
		List<String> ret = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < in.length(); ++i) {
			char cur = in.charAt(i);

			if (cur == '.' || cur == '!' || cur == '?') {
				if (!sb.toString().equals(" ")) {
					ret.add(sb.toString());
				}
				sb = new StringBuilder();
			} else if (Character.isLetter(cur) || cur == ' ') {
				sb.append(cur);
			}
		}
		ret.add(sb.toString());
		return ret;
	}

	/*
	 * Method to make bigrams from a given string. Output is in the form
	 * "word1, word2"
	 */
	private List<String> makeBigrams(String in) {
		if (in.length() == 0)
			return null;

		List<String> ret = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(in);

		String prev = st.nextToken();
		String cur = "";
		while (st.hasMoreTokens()) {
			cur = st.nextToken();
			ret.add(this.makeBigram(prev, cur));
			prev = cur;
		}
		return ret;
	}

	/*
	 * A method to make a bigram out of two given words
	 */
	private String makeBigram(String one, String two) {
		return one + ", " + two;
	}

	public String reduce(String key, Iterable<Integer> values) {

		int value = 0;
		for (Integer val : values) {
			value += val;
		}

		return (key + " " + value);
	}

	public String map(long key, String value) {
		
		return value;
	}
	
	public List<String> reduce2(List<String> values) {
		List<String> ret = new ArrayList<String>();
		PriorityQueue<BigramBox> pq = new PriorityQueue<BigramBox>(10);
		
		for (String t: values) {
			pq.add(new BigramBox(t));
		}
		
		for (int i = 0; i < 10; ++i) {
			BigramBox b = pq.remove();
			ret.add(b.bigram + " " + b.occurrences);
		}
		return ret;
	}  // End method "reduce"
	
	private static class BigramBox implements Comparable<BigramBox> {
		public String bigram;
		public int occurrences;
		
		public BigramBox(String big) {
			String[] vals = big.split(" ");
			bigram = vals[0] + " " + vals[1];
			occurrences = Integer.parseInt(vals[2]);
		}
		
		public int compareTo(BigramBox rhs) {
			return rhs.occurrences - this.occurrences;
		}
		
		public String toString() {
			return "<" + bigram + ">, " + occurrences;
		}
	}
}