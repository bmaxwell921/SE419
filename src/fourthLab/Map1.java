package fourthLab;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map1 extends Mapper<LongWritable, Text, Text, Text> {

	private int kShingles = 9;
	private int numMinHashes = 6;
	
	private static BufferedWriter bw;
	
	private int[] bigSeedArray = {42, 17, 100, 7, 13, 21, 91, 72, 65, 75};
	
//	public static void main(String[] args) throws IOException, InterruptedException {
//		bw = new BufferedWriter(new FileWriter("src/fourthLab/Map1Out.txt"));
//		ArrayList<String> in = MapReduceUtil.readMap("src/fourthLab/input.txt");
//		
//		Map1 m = new Map1();
//		for (String s: in) {
//			m.map(new LongWritable(1), new Text(s), null);
//		}
//		
//		bw.flush();
//		bw.close();
//	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString(), "-");
		String id = st.nextToken();
		String doc = st.nextToken();
		List<String> shingles = this.makeShingles(doc, kShingles);
		
		int[] seeds = new int[numMinHashes];
		
		for (int i = 0; i < numMinHashes; ++i) {
			seeds[i] = bigSeedArray[i];
		}
		
		int[] minHashes = new int[numMinHashes];
		
		for (int i = 0; i < numMinHashes; ++i) {
			int minHash = this.makeMinHash(shingles, seeds[i]);
			minHashes[i] = minHash;
		}
		
		//Emit ALL THE MINHASHES as the key
		StringBuilder sb = new StringBuilder();
		for (int minHash : minHashes) {
			sb.append(minHash);
			sb.append(",");
		}
		if (context == null) {
			bw.write(sb.toString() + "\t" + value.toString());
			bw.newLine();
		} else {
			context.write(new Text(sb.toString()), value);
		}
	}
	
	private int makeMinHash(List<String> shingles, int seed) {
		int minHash = Runner.hash(shingles.get(0).getBytes(), seed);
		for (int i = 1; i < shingles.size(); ++i) {
			int curHash = Runner.hash(shingles.get(i).getBytes(), seed);
			if (curHash < minHash) {
				minHash = curHash;
			}
		}
		return minHash;
	}

	private List<String> makeShingles(String value, int kShingles) {
		List<String> shingles = new ArrayList<String>();
		
		for (int i = 0; i < value.length() - kShingles + 1; ++i) {
			shingles.add(value.substring(i, i + kShingles));
		}
		
		return shingles;
	}
}
