package thirdLab.exp2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReduce2 extends Reducer <Text, Text, Text, Text>{

	private static BufferedWriter bw;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		bw = new BufferedWriter(new FileWriter("src/thirdLab/exp2/Reducer2Out.txt"));
		BufferedReader br = new BufferedReader(new FileReader("src/thirdLab/exp2/Map2Out.txt"));
		
		Map<String, List<Text>> values = new HashMap<String, List<Text>>();
		String read = "";
		
		while ((read = br.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(read);
			String key = st.nextToken();
			String value = getRestOfTokens(st);
			
			List<Text> val = values.get(key);
			if (val == null) {
				val = new ArrayList<Text>();
			}
			val.add(new Text(value));
			values.put(key, val);
		}
		
		TReduce2 r = new TReduce2();
		
		for (String key: values.keySet()) {
			r.reduce(new Text(key), values.get(key), null);
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
	//TODO not working
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeNode root = new TreeNode(key.toString(), new ArrayList<TreeNode>());
		Set<String> oneHops;
		for (Text t: values) {
			root.children.add(this.makeTreeNode(t.toString()));
			
		}
		oneHops = makeOneHops(root.children);
		Set<List<String>> triangles = this.buildTriangles(root, oneHops);
		
		for (List<String> l: triangles) {
			if (context == null) {
				bw.write(this.listToString(l));
				bw.newLine();
			} else {
				context.write(new Text(this.listToString(l)), new Text(""));
			}
		}
	}

	private TreeNode makeTreeNode(String in) {
		StringTokenizer st = new StringTokenizer(in);
		TreeNode ret = new TreeNode(st.nextToken(), new ArrayList<TreeNode>());
		
		while (st.hasMoreTokens()) {
			ret.children.add(new TreeNode(st.nextToken(), null));
		}
		
		return ret;
	}
	
	private Set<String> makeOneHops(List<TreeNode> children) {
		Set<String> oneHops = new HashSet<String>();
		for (TreeNode tn: children) {
			oneHops.add(tn.data);
		}
		return oneHops;
	}

	private Set<List<String>> buildTriangles(TreeNode root, Set<String> oneHops) {
		Set<List<String>> triangles = new HashSet<List<String>>();

		for (TreeNode oneHop: root.children) {
			List<String> curTri = new ArrayList<String>();
			curTri.add(root.data);
			curTri.add(oneHop.data);
			for (TreeNode twoHop: oneHop.children) {
				if (oneHops.contains(twoHop.data)) {
					curTri.add(twoHop.data);
					Collections.sort(curTri);
					triangles.add(curTri);
					curTri = new ArrayList<String>();
					curTri.add(root.data);
					curTri.add(oneHop.data);
				}
			}
		}
		return triangles;
	}	
	
	private String listToString(List<String> list) {
		StringBuilder sb = new StringBuilder();
		for (String s: list) {
			sb.append(s + " ");
		}
		return sb.toString();
	}
	
	private class TreeNode {
		public String data;
		public List<TreeNode> children;
		
		public TreeNode(String d, List<TreeNode> c) {
			data = d;
			children = c;
		}
		
		@Override
		public String toString() {
			return data + " " + children.toString();
		}
	}
}
